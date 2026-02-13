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

    logger.info("🧹 Pruning signals older than 21 days...")
    cutoff = (datetime.now() - timedelta(days=21)).isoformat()
    supabase.table("signal_watchlist").delete().lt("last_updated", cutoff).execute()

    # Fetch unique symbols available in 1Day timeframe
    active_tickers_resp = supabase.table("market_data").select("symbol").eq("timeframe", "1Day").execute()
    symbols = list(set([item['symbol'] for item in active_tickers_resp.data]))

    logger.info(f"🔎 Scanning {len(symbols)} symbols...")

    bulk_results = []
    batch_size = 100

    for symbol in symbols:
        try:
            # Query 100 days of data (Optimized by your new Index)
            daily_data = supabase.table("market_data").select("*").eq("symbol", symbol).eq("timeframe", "1Day").order(
                "timestamp", desc=True).limit(100).execute()

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

            # Directional Entry Logic (RSI Touch)
            direction = 'LONG' if curr_rsi <= 30 else ('SHORT' if curr_rsi >= 70 else None)

            # Fetch existing to check for trailing extreme_price
            existing = supabase.table("signal_watchlist").select("*").eq("symbol", symbol).execute()

            if direction or existing.data:
                final_dir = direction if direction else existing.data[0]['direction']
                ext_price = existing.data[0]['extreme_price'] if existing.data else df_daily['close'].iloc[-1]

                # Rule: Update extreme price for stop loss tracking
                if final_dir == 'LONG':
                    ext_price = min(df_daily['low'].iloc[-1], ext_price)
                else:
                    ext_price = max(df_daily['high'].iloc[-1], ext_price)

                # The 4 Hard Rules
                rsi_up = curr_rsi > prev_rsi if final_dir == 'LONG' else curr_rsi < prev_rsi
                w_rsi_up = curr_w_rsi > prev_w_rsi if final_dir == 'LONG' else curr_w_rsi < prev_w_rsi
                macd_up = curr_macd > prev_macd if final_dir == 'LONG' else curr_macd < prev_macd

                is_ready = all([rsi_up, w_rsi_up, macd_up])

                # Prepare record for JSONB Bulk Upsert
                bulk_results.append({
                    "symbol": symbol,
                    "direction": final_dir,
                    "rsi_touch_value": float(curr_rsi),
                    "extreme_price": float(ext_price),
                    "atr": float(atr_val),
                    "is_ready": is_ready,
                    "logic_trail": {
                        "d_rsi": float(curr_rsi),
                        "w_rsi": float(curr_w_rsi),
                        "macd_slope": "up" if macd_up else "down"
                    },
                    "last_updated": datetime.now().isoformat()
                })

                # Flush Batch
                if len(bulk_results) >= batch_size:
                    supabase.table("signal_watchlist").upsert(bulk_results, on_conflict="symbol,direction").execute()
                    bulk_results = []

        except Exception as e:
            logger.error(f"❌ Error {symbol}: {e}")

    # Final Flush
    if bulk_results:
        supabase.table("signal_watchlist").upsert(bulk_results, on_conflict="symbol,direction").execute()


if __name__ == "__main__":
    run_sidbot_scanner()