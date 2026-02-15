import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import AverageTrueRange
from populate_db import get_clients
import math

# --- 0. LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def get_weekly_rsi_resampled(df_daily):
    """Simulates TradingView's Weekly RSI via resampling daily data."""
    temp_df = df_daily.copy()
    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
    temp_df.set_index('timestamp', inplace=True)
    df_weekly = temp_df.resample('W-FRI').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
    }).dropna()
    if len(df_weekly) < 15: return None, None
    rsi_weekly = RSIIndicator(close=df_weekly['close'], window=14).rsi()
    return rsi_weekly.iloc[-1], rsi_weekly.iloc[-2]


def detect_reversal_pattern(df, direction):
    """Detects simple reversal patterns (Hammer for Long, Shooting Star for Short)."""
    last_row = df.iloc[-1]
    body = abs(last_row['close'] - last_row['open'])
    wick_high = last_row['high'] - max(last_row['open'], last_row['close'])
    wick_low = min(last_row['open'], last_row['close']) - last_row['low']
    if direction == 'LONG':
        return 1 if (wick_low > body * 2 and wick_high < body) else 0
    return 1 if (wick_high > body * 2 and wick_low < body) else 0


def detect_macd_crossover(df, direction):
    """Detects if MACD Line has crossed the Signal Line."""
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    macd_line = exp1 - exp2
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    if direction == 'LONG':
        return 1 if (macd_line.iloc[-2] < signal_line.iloc[-2] and macd_line.iloc[-1] > signal_line.iloc[-1]) else 0
    return 1 if (macd_line.iloc[-2] > signal_line.iloc[-2] and macd_line.iloc[-1] < signal_line.iloc[-1]) else 0


def run_sidbot_scanner():
    clients = get_clients()
    supabase = clients['supabase_client']

    # 1. FETCH MASTER LIST & MARKET CONTEXT
    tickers_resp = supabase.table("ticker_metadata").select("symbol").execute()
    symbols = [item['symbol'] for item in tickers_resp.data]

    spy_data = supabase.table("market_data").select("close").eq("symbol", "SPY").eq("timeframe", "1Day").order(
        "timestamp", desc=True).limit(2).execute()
    spy_up = spy_data.data[0]['close'] > spy_data.data[1]['close'] if len(spy_data.data) > 1 else True

    for symbol in symbols:
        try:
            # 2. GET DATA (100 days for indicators)
            daily_data = supabase.table("market_data").select("*").eq("symbol", symbol).eq("timeframe", "1Day").order(
                "timestamp", desc=True).limit(100).execute()
            if len(daily_data.data) < 35: continue
            df_daily = pd.DataFrame(daily_data.data).iloc[::-1]

            # 3. CALCULATE INDICATORS
            rsi_daily_ser = RSIIndicator(close=df_daily['close']).rsi()
            macd_obj = MACD(close=df_daily['close'])
            macd_line = macd_obj.macd()
            atr_val = AverageTrueRange(high=df_daily['high'], low=df_daily['low'],
                                       close=df_daily['close']).average_true_range().iloc[-1]
            curr_rsi, prev_rsi = rsi_daily_ser.iloc[-1], rsi_daily_ser.iloc[-2]
            curr_macd, prev_macd = macd_line.iloc[-1], macd_line.iloc[-2]
            curr_w_rsi, prev_w_rsi = get_weekly_rsi_resampled(df_daily)
            if curr_w_rsi is None: continue

            # 4. MOMENTUM ROOM DIRECTION (45/55)
            direction = None
            if curr_rsi <= 45:
                direction = 'LONG'
            elif curr_rsi >= 55:
                direction = 'SHORT'

            existing = supabase.table("signal_watchlist").select("*").eq("symbol", symbol).execute()
            if direction or existing.data:
                final_dir = direction if direction else existing.data[0]['direction']

                # 5. GATES (The Turn)
                d_rsi_ok = (curr_rsi > prev_rsi) if final_dir == 'LONG' else (curr_rsi < prev_rsi)
                w_rsi_ok = (curr_w_rsi > prev_w_rsi) if final_dir == 'LONG' else (curr_w_rsi < prev_w_rsi)
                macd_ok = (curr_macd > prev_macd) if final_dir == 'LONG' else (curr_macd < prev_macd)

                # 6. EARNINGS COUNTDOWN
                earnings_resp = supabase.table("earnings_calendar").select("report_date").eq("symbol", symbol).gte(
                    "report_date", datetime.now().date().isoformat()).order("report_date").limit(1).execute()
                next_earnings_date, days_to_earnings = None, 999
                if earnings_resp.data:
                    next_earnings_date = earnings_resp.data[0]['report_date']
                    days_to_earnings = (
                                datetime.strptime(next_earnings_date, '%Y-%m-%d').date() - datetime.now().date()).days

                is_ready = all([d_rsi_ok, w_rsi_ok, macd_ok, (days_to_earnings > 3)])

                # 7. CONVICTION SCORE (Score 0-3)
                total_score = int(detect_reversal_pattern(df_daily, final_dir) + (
                    1 if (spy_up if final_dir == 'LONG' else not spy_up) else 0) + detect_macd_crossover(df_daily,
                                                                                                         final_dir))

                logic_trail = {"d_rsi": round(float(curr_rsi), 1), "w_rsi": round(float(curr_w_rsi), 1),
                               "macd_ready": bool(macd_ok), "score": total_score}

                # 8. DYNAMIC STOP LOSS
                # Current bar extremes
                low_val = float(df_daily['low'].iloc[-1])
                high_val = float(df_daily['high'].iloc[-1])

                # Initialize with current values if no existing data
                ext_price = low_val if final_dir == 'LONG' else high_val

                if existing.data:
                    # Get the previously stored extreme price from Supabase
                    stored_extreme = existing.data[0].get('extreme_price')

                    if stored_extreme is not None:
                        if final_dir == 'LONG':
                            # For LONG: Only update if today's low is LOWER than the stored extreme
                            ext_price = min(low_val, float(stored_extreme))
                        else:
                            # For SHORT: Only update if today's high is HIGHER than the stored extreme
                            ext_price = max(high_val, float(stored_extreme))


                # 1. Capture current closing price
                current_close = float(df_daily['close'].iloc[-1])

                # 2. Re-apply your specific stop loss rounding logic
                def calculate_formatted_stop(price, direction):
                    if direction == 'LONG':
                        # Long: 150.50 -> 150; 150.00 -> 149
                        return float(price - 1 if price.is_integer() else math.floor(price))
                    else:
                        # Short: 150.25 -> 151; 150.00 -> 151
                        return float(price + 1 if price.is_integer() else math.ceil(price))

                final_stop = calculate_formatted_stop(ext_price, final_dir)

                #9 UPSERT TO SUPABASE
                supabase.table("signal_watchlist").upsert({
                    "symbol": symbol,
                    "extreme_price": float(ext_price),
                    "stop_loss": final_stop,
                    "entry_price": current_close,  # NEW: Save current close as entry basis
                    "is_ready": bool(is_ready),
                    "last_updated": datetime.now().isoformat(),
                    "next_earnings": next_earnings_date,
                    "logic_trail": logic_trail,
                }, on_conflict="symbol").execute()


        except Exception as e:
            logger.error(f"❌ Error scanning {symbol}: {e}")


if __name__ == "__main__":
    run_sidbot_scanner()