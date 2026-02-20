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
from pref_watchlist import PREF_WATCHLIST

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

def is_on_preferred_watchlist(symbol):
    """Checks if the symbol is on the preferred watchlist."""
    return symbol in PREF_WATCHLIST


def run_sidbot_scanner():
    clients = get_clients()
    supabase = clients['supabase_client']

    # Alpaca setup for live data
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockSnapshotRequest
    alpaca_client = StockHistoricalDataClient(
        os.getenv("APCA_API_KEY_ID"),
        os.getenv("APCA_API_SECRET_KEY")
    )

    # 1. FETCH MASTER LIST & MARKET CONTEXT
    tickers_resp = supabase.table("ticker_reference").select("symbol").execute()
    symbols = [item['symbol'] for item in tickers_resp.data]

    spy_data = supabase.table("market_data").select("close").eq("symbol", "SPY").eq("timeframe", "1d").order(
        "timestamp", desc=True).limit(2).execute()
    spy_up = spy_data.data[0]['close'] > spy_data.data[1]['close'] if len(spy_data.data) > 1 else True

    for symbol in symbols:
        try:
            # 2. GET HISTORICAL DATA
            daily_data = supabase.table("market_data").select("*").eq("symbol", symbol).eq("timeframe", "1d").order(
                "timestamp", desc=True).limit(100).execute()
            if len(daily_data.data) < 50: continue
            df_daily = pd.DataFrame(daily_data.data).iloc[::-1]

            # 3. APPEND LIVE BAR FROM ALPACA
            snapshot = alpaca_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=symbol))
            latest_bar = snapshot[symbol].daily_bar
            if latest_bar:
                live_row = pd.DataFrame([{
                    'timestamp': latest_bar.timestamp.isoformat(),
                    'open': float(latest_bar.open),
                    'high': float(latest_bar.high),
                    'low': float(latest_bar.low),
                    'close': float(latest_bar.close),
                    'volume': int(latest_bar.volume),
                    'symbol': symbol
                }])
                df_daily = pd.concat([df_daily, live_row], ignore_index=True)

            # 4. DEDUPLICATE (Ensure no double bars for 'today')
            df_daily['timestamp'] = pd.to_datetime(df_daily['timestamp'])
            df_daily = df_daily.sort_values('timestamp').drop_duplicates('timestamp', keep='last')

            # 5. CALCULATE INDICATORS
            rsi_daily_ser = RSIIndicator(close=df_daily['close']).rsi()
            macd_obj = MACD(close=df_daily['close'])
            macd_line = macd_obj.macd()

            curr_rsi, prev_rsi = rsi_daily_ser.iloc[-1], rsi_daily_ser.iloc[-2]
            curr_macd, prev_macd = macd_line.iloc[-1], macd_line.iloc[-2]

            curr_w_rsi, prev_w_rsi = get_weekly_rsi_resampled(df_daily)
            if curr_w_rsi is None: continue

            # --- 6. SID METHOD DIRECTIONAL LOGIC ---
            # Lookback: Did RSI touch <= 30 or >= 70 in the last 28 bars?
            rsi_lookback = rsi_daily_ser.iloc[-28:]
            touched_oversold = (rsi_lookback <= 30).any()
            touched_overbought = (rsi_lookback >= 70).any()

            direction = None
            if touched_oversold and curr_rsi <= 45:
                direction = 'LONG'
            elif touched_overbought and curr_rsi >= 55:
                direction = 'SHORT'

            existing = supabase.table("sid_method_signal_watchlist").select("*").eq("symbol", symbol).execute()
            if not direction and not existing.data:
                continue

            final_dir = direction if direction else existing.data[0]['direction']

            # VALIDATION & CLEANUP: Remove if moved past "Momentum Room"
            if (final_dir == 'LONG' and curr_rsi > 45) or (final_dir == 'SHORT' and curr_rsi < 55):
                logger.info(f"🧹 Removing {symbol} from watchlist: RSI {curr_rsi:.1f} left room.")
                supabase.table("sid_method_signal_watchlist").delete().eq("symbol", symbol).execute()
                continue

            # 7. GATES (The Turn)
            d_rsi_ok = (curr_rsi > prev_rsi) if final_dir == 'LONG' else (curr_rsi < prev_rsi)
            w_rsi_ok = (curr_w_rsi > prev_w_rsi) if final_dir == 'LONG' else (curr_w_rsi < prev_w_rsi)
            macd_ok = (curr_macd > prev_macd) if final_dir == 'LONG' else (curr_macd < prev_macd)

            # 8. EARNINGS & CONVICTION
            earnings_resp = supabase.table("earnings_calendar").select("report_date").eq("symbol", symbol).gte(
                "report_date", datetime.now().date().isoformat()).order("report_date").limit(1).execute()
            next_earnings_date, days_to_earnings = None, 999
            if earnings_resp.data:
                next_earnings_date = earnings_resp.data[0]['report_date']
                days_to_earnings = (
                            datetime.strptime(next_earnings_date, '%Y-%m-%d').date() - datetime.now().date()).days

            is_ready = all([d_rsi_ok, w_rsi_ok, macd_ok, (days_to_earnings > 14)])

            # Conviction Scoring
            macd_cross = bool(detect_macd_crossover(df_daily, final_dir))
            pattern_confirmed = bool(detect_reversal_pattern(df_daily, final_dir))
            spy_alignment = bool(spy_up if final_dir == 'LONG' else not spy_up)
            preferred_watchlist = is_on_preferred_watchlist(symbol)
            total_score = int(macd_cross + pattern_confirmed + spy_alignment + preferred_watchlist)

            # 9. UPSERT TO SUPABASE
            supabase.table("sid_method_signal_watchlist").upsert({
                "symbol": symbol,
                "direction": final_dir,
                "entry_price": float(df_daily['close'].iloc[-1]),
                "market_score": total_score,
                "is_ready": bool(is_ready),
                "last_updated": datetime.now().isoformat(),
                "next_earnings": next_earnings_date,
                "preferred_watchlist": preferred_watchlist,
                "logic_trail": {
                    "d_rsi": round(float(curr_rsi), 1),
                    "touched_extreme": bool(touched_oversold if final_dir == 'LONG' else touched_overbought)
                }
            }, on_conflict="symbol").execute()

        except Exception as e:
            logger.error(f"❌ Error scanning {symbol}: {e}")


if __name__ == "__main__":
    run_sidbot_scanner()