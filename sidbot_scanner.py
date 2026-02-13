import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import AverageTrueRange
from populate_db import get_clients

# --- 0. LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def get_weekly_rsi_resampled(df_daily):
    """Simulates TradingView Weekly RSI by resampling daily data."""
    temp_df = df_daily.copy()
    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
    temp_df.set_index('timestamp', inplace=True)
    df_weekly = temp_df.resample('W-FRI').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}).dropna()
    if len(df_weekly) < 15: return None, None
    rsi_ser = RSIIndicator(close=df_weekly['close'], window=14).rsi()
    return rsi_ser.iloc[-1], rsi_ser.iloc[-2]


def run_sidbot_scanner():
    clients = get_clients()
    supabase = clients['supabase_client']

    # 1. FETCH ALL SYMBOLS FROM METADATA (Ensures we scan all 1,013)
    tickers_resp = supabase.table("ticker_metadata").select("symbol").execute()
    symbols = [item['symbol'] for item in tickers_resp.data]
    logger.info(f"🔎 Scanning {len(symbols)} symbols for SidBot criteria...")

    for symbol in symbols:
        try:
            # 2. GET DAILY DATA (Need 100 days for MACD/Weekly Resample)
            data_resp = supabase.table("market_data").select("*").eq("symbol", symbol).eq("timeframe", "1Day").order(
                "timestamp", desc=True).limit(100).execute()
            if len(data_resp.data) < 30: continue
            df = pd.DataFrame(data_resp.data).iloc[::-1]

            # 3. CALCULATE INDICATORS
            rsi_daily = RSIIndicator(close=df['close']).rsi()
            macd_line = MACD(close=df['close']).macd()
            atr = AverageTrueRange(high=df['high'], low=df['low'], close=df['close']).average_true_range().iloc[-1]

            curr_rsi, prev_rsi = rsi_daily.iloc[-1], rsi_daily.iloc[-2]
            curr_macd, prev_macd = macd_line.iloc[-1], macd_line.iloc[-2]
            curr_w_rsi, prev_w_rsi = get_weekly_rsi_resampled(df)
            if curr_w_rsi is None: continue

            # 4. DIRECTIONAL LOGIC
            final_dir = None
            if curr_rsi <= 30:
                final_dir = 'LONG'
            elif curr_rsi >= 70:
                final_dir = 'SHORT'

            # Check Watchlist for existing trades
            existing = supabase.table("signal_watchlist").select("*").eq("symbol", symbol).execute()
            if existing.data and not final_dir:
                final_dir = existing.data[0]['direction']

                # --- THE TRIPLE SLOPE TURN FIX ---
                if final_dir == 'LONG':
                    # All indicators must be pointing UP
                    ready = (curr_rsi > prev_rsi) and (curr_w_rsi > prev_w_rsi) and (curr_macd > prev_macd)
                else:  # SHORT
                    # All indicators must be pointing DOWN
                    # If RSI is rising (like your AVT case), this will return False
                    ready = (curr_rsi < prev_rsi) and (curr_w_rsi < prev_w_rsi) and (curr_macd < prev_macd)

                # Only upsert if ready matches your 4-Hard-Rules criteria
                supabase.table("signal_watchlist").upsert({
                    "symbol": symbol,
                    "direction": final_dir,
                    "is_ready": ready,  # This now strictly requires the turn
                    "last_updated": datetime.now().isoformat()
                    # ... other fields
                }, on_conflict="symbol").execute()
                # 6. DYNAMIC STOP LOSS (Extreme Price)
                low_val, high_val = df['low'].iloc[-1], df['high'].iloc[-1]
                if existing.data:
                    old_ext = existing.data[0]['extreme_price']
                    ext_price = min(low_val, old_ext) if final_dir == 'LONG' else max(high_val, old_ext)
                else:
                    ext_price = low_val if final_dir == 'LONG' else high_val

                # 7. UPSERT
                supabase.table("signal_watchlist").upsert({
                    "symbol": symbol, "direction": final_dir, "is_ready": is_ready,
                    "extreme_price": float(ext_price), "rsi_touch_value": float(curr_rsi),
                    "atr": float(atr), "last_updated": datetime.now().isoformat()
                }, on_conflict="symbol").execute()

        except Exception as e:
            logger.error(f"❌ Error scanning {symbol}: {e}")


if __name__ == "__main__":
    run_sidbot_scanner()