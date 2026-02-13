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

    # Optimized Query: Only fetch symbols from the 1Day timeframe
    active_tickers_resp = supabase.table("market_data").select("symbol").eq("timeframe", "1Day").execute()
    symbols = list(set([item['symbol'] for item in active_tickers_resp.data]))

    # Fetch entire existing watchlist to avoid O(N) queries in the loop
    watchlist_resp = supabase.table("signal_watchlist").select("*").execute()
    watchlist = {item['symbol']: item for item in watchlist_resp.data}

    logger.info(f"🔎 Scanning {len(symbols)} symbols...")

    bulk_results = []
    batch_size = 50  # Smaller batch for stability with 17M rows

    for symbol in symbols:
        try:
            # Increased limit to 250 for RSI accuracy (1 year of daily bars)
            daily_data = supabase.table("market_data").select("*").eq("symbol", symbol).eq("timeframe", "1Day").order(
                "timestamp", desc=True).limit(250).execute()

            if len(daily_data.data) < 50: continue  # Need enough data for stable indicators
            df_daily = pd.DataFrame(daily_data.data).iloc[::-1]
            
            # Ensure numeric types for indicators
            for col in ['open', 'high', 'low', 'close']:
                df_daily[col] = pd.to_numeric(df_daily[col], errors='coerce')

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

            # Check existing watchlist entry from memory
            existing_entry = watchlist.get(symbol)

            if direction or existing_entry:
                final_dir = direction if direction else existing_entry['direction']
                ext_price = existing_entry['extreme_price'] if existing_entry else df_daily['close'].iloc[-1]

                # Update Extreme Price for Stop Loss
                if final_dir == 'LONG':
                    ext_price = min(df_daily['low'].iloc[-1], ext_price)
                else:
                    ext_price = max(df_daily['high'].iloc[-1], ext_price)

                # The 4 Hard Rules
                rsi_up = curr_rsi > prev_rsi if final_dir == 'LONG' else curr_rsi < prev_rsi
                w_rsi_up = curr_w_rsi > prev_w_rsi if final_dir == 'LONG' else curr_w_rsi < prev_w_rsi
                macd_up = curr_macd > prev_macd if final_dir == 'LONG' else curr_macd < prev_macd

                is_ready = all([rsi_up, w_rsi_up, macd_up])

                # Prepare for Bulk Upsert
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
                        "macd_ready": bool(macd_up)
                    },
                    "last_updated": datetime.now().isoformat()
                })

                if len(bulk_results) >= batch_size:
                    supabase.table("signal_watchlist").upsert(bulk_results, on_conflict="symbol,direction").execute()
                    bulk_results = []

        except Exception as e:
            logger.error(f"❌ Error scanning {symbol}: {e}")

    # Final flush
    if bulk_results:
        supabase.table("signal_watchlist").upsert(bulk_results, on_conflict="symbol,direction").execute()
        logger.info(f"🏁 Final scan complete. Watchlist updated.")


if __name__ == "__main__":
    run_sidbot_scanner()