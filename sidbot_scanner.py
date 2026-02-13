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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("sidbot_scanner.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def get_weekly_rsi_resampled(df_daily):
    temp_df = df_daily.copy()
    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
    temp_df.set_index('timestamp', inplace=True)
    df_weekly = temp_df.resample('W-FRI').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
    }).dropna()
    if len(df_weekly) < 15: return None, None
    rsi_weekly = RSIIndicator(close=df_weekly['close'], window=14).rsi()
    return rsi_weekly.iloc[-1], rsi_weekly.iloc[-2]


def run_sidbot_scanner():
    clients = get_clients()
    supabase = clients['supabase_client']

    # 1. PRUNE: Remove signals older than 21 days
    logger.info("🧹 Pruning signals older than 21 days...")
    cutoff = (datetime.now() - timedelta(days=21)).isoformat()
    supabase.table("signal_watchlist").delete().lt("last_updated", cutoff).execute()

    # 2. FIXED: Pull from Master List (ticker_metadata) instead of market_data
    # This ensures we see all 2,798 symbols regardless of database limits
    logger.info("📡 Fetching Master Ticker List...")
    meta_resp = supabase.table("ticker_metadata").select("symbol").execute()
    symbols = [item['symbol'] for item in meta_resp.data]

    logger.info(f"🔎 Scanning {len(symbols)} symbols for SidBot criteria...")

    batch_to_upsert = []

    for symbol in symbols:
        try:
            # Indexed Query: Fast lookup for specific symbol
            daily_data = supabase.table("market_data").select("*") \
                .eq("symbol", symbol).eq("timeframe", "1Day") \
                .order("timestamp", desc=True).limit(100).execute()

            if len(daily_data.data) < 26: continue
            df_daily = pd.DataFrame(daily_data.data).iloc[::-1]

            # Indicators
            rsi_ser = RSIIndicator(close=df_daily['close']).rsi()
            macd_line = MACD(close=df_daily['close']).macd()
            atr_val = AverageTrueRange(high=df_daily['high'], low=df_daily['low'],
                                       close=df_daily['close']).average_true_range().iloc[-1]

            curr_rsi, prev_rsi = rsi_ser.iloc[-1], rsi_ser.iloc[-2]
            curr_macd, prev_macd = macd_line.iloc[-1], macd_line.iloc[-2]
            curr_w_rsi, prev_w_rsi = get_weekly_rsi_resampled(df_daily)

            if curr_w_rsi is None: continue

            # Directional Entry Logic
            direction = 'LONG' if curr_rsi <= 30 else ('SHORT' if curr_rsi >= 70 else None)

            # Fetch existing to track extreme price
            existing = supabase.table("signal_watchlist").select("*").eq("symbol", symbol).execute()

            if direction or existing.data:
                final_dir = direction if direction else existing.data[0]['direction']
                ext_price = existing.data[0]['extreme_price'] if existing.data else df_daily['close'].iloc[-1]

                if final_dir == 'LONG':
                    ext_price = min(df_daily['low'].iloc[-1], ext_price)
                else:
                    ext_price = max(df_daily['high'].iloc[-1], ext_price)

                rsi_up = curr_rsi > prev_rsi if final_dir == 'LONG' else curr_rsi < prev_rsi
                w_rsi_up = curr_w_rsi > prev_w_rsi if final_dir == 'LONG' else curr_w_rsi < prev_w_rsi
                macd_up = curr_macd > prev_macd if final_dir == 'LONG' else curr_macd < prev_macd

                is_ready = all([rsi_up, w_rsi_up, macd_up])

                batch_to_upsert.append({
                    "symbol": symbol,
                    "direction": final_dir,
                    "rsi_touch_value": float(curr_rsi),
                    "extreme_price": float(ext_price),
                    "atr": float(atr_val),
                    "is_ready": is_ready,
                    "logic_trail": {
                        "d_rsi": float(curr_rsi),
                        "w_rsi": float(curr_w_rsi),
                        "macd_ready": bool(macd_up)
                    },
                    "last_updated": datetime.now().isoformat()
                })

                # Bulk Upsert
                if len(batch_to_upsert) >= 50:
                    supabase.table("signal_watchlist").upsert(batch_to_upsert, on_conflict="symbol,direction").execute()
                    batch_to_upsert = []

        except Exception as e:
            logger.error(f"❌ Error scanning {symbol}: {e}")

    # Final Flush
    if batch_to_upsert:
        supabase.table("signal_watchlist").upsert(batch_to_upsert, on_conflict="symbol,direction").execute()

    logger.info("🏁 Scan Complete.")


if __name__ == "__main__":
    run_sidbot_scanner()