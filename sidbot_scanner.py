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
    """Resamples Daily data to Weekly to calculate 14-period RSI."""
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

    # 1. PRUNE: Remove signals older than 28 days
    logger.info("🧹 Pruning signals older than 28 days...")
    cutoff = (datetime.now() - timedelta(days=28)).isoformat()
    supabase.table("signal_watchlist").delete().lt("last_updated", cutoff).execute()

    # 2. FETCH FULL MASTER LIST (Bypassing 1000 row limit)
    logger.info("📡 Fetching Full Master Ticker List...")
    symbols = []
    offset = 0
    while True:
        resp = supabase.table("ticker_metadata").select("symbol").range(offset, offset + 999).execute()
        batch = [item['symbol'] for item in resp.data]
        symbols.extend(batch)
        if len(batch) < 1000: break
        offset += 1000

    # 3. PRE-LOAD SIGNAL WATCHLIST (Optimization: 1 request instead of 2800)
    logger.info("⚡ Pre-loading existing signals into memory...")
    watchlist_resp = supabase.table("signal_watchlist").select("*").execute()
    watchlist_map = {item['symbol']: item for item in watchlist_resp.data}

    logger.info(f"🔎 Scanning {len(symbols)} symbols...")
    bulk_results = []

    for symbol in symbols:
        try:
            # Query price bars (Optimized by your Database Index)
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

            # Entry Logic (RSI Touch)
            direction = 'LONG' if curr_rsi <= 30 else ('SHORT' if curr_rsi >= 70 else None)

            # Use Local Map instead of API call
            existing_data = watchlist_map.get(symbol)

            if direction or existing_data:
                final_dir = direction if direction else existing_data['direction']
                ext_price = existing_data['extreme_price'] if existing_data else df_daily['close'].iloc[-1]

                # Update Trailing Stop Loss logic
                if final_dir == 'LONG':
                    ext_price = min(df_daily['low'].iloc[-1], ext_price)
                else:
                    ext_price = max(df_daily['high'].iloc[-1], ext_price)

                # The 4 Hard Rules
                rsi_up = curr_rsi > prev_rsi if final_dir == 'LONG' else curr_rsi < prev_rsi
                w_rsi_up = curr_w_rsi > prev_w_rsi if final_dir == 'LONG' else curr_w_rsi < prev_w_rsi
                macd_up = curr_macd > prev_macd if final_dir == 'LONG' else curr_macd < prev_macd

                is_ready = all([rsi_up, w_rsi_up, macd_up])

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

                # Batch Upsert (Every 100 results)
                if len(bulk_results) >= 100:
                    supabase.table("signal_watchlist").upsert(bulk_results, on_conflict="symbol,direction").execute()
                    bulk_results = []

        except Exception as e:
            logger.error(f"❌ Error scanning {symbol}: {e}")

    # Final Flush
    if bulk_results:
        supabase.table("signal_watchlist").upsert(bulk_results, on_conflict="symbol,direction").execute()

    logger.info("🏁 Final scan complete. Watchlist updated.")


if __name__ == "__main__":
    run_sidbot_scanner()