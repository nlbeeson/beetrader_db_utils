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

    # Pre-load maps for efficiency
    watchlist_resp = supabase.table("signal_watchlist").select("*").execute()
    watchlist_map = {item['symbol']: item for item in watchlist_resp.data}
    earn_resp = supabase.table("earnings_calendar").select("*").execute()
    earnings_map = {item['symbol']: item['report_date'] for item in earn_resp.data}

    active_tickers_resp = supabase.table("market_data").select("symbol").eq("timeframe", "1Day").execute()
    symbols = list(set([item['symbol'] for item in active_tickers_resp.data]))

    logger.info(f"🔎 Scanning {len(symbols)} symbols...")
    bulk_results = []

    for symbol in symbols:
        try:
            daily_data = supabase.table("market_data").select("*") \
                .eq("symbol", symbol).eq("timeframe", "1Day") \
                .order("timestamp", desc=True).limit(100).execute()

            if len(daily_data.data) < 26: continue
            df_daily = pd.DataFrame(daily_data.data).iloc[::-1]

            # Indicators
            rsi_ser = RSIIndicator(close=df_daily['close']).rsi()
            macd_obj = MACD(close=df_daily['close'])
            macd_line, signal_line = macd_obj.macd(), macd_obj.macd_signal()
            atr_val = AverageTrueRange(high=df_daily['high'], low=df_daily['low'],
                                       close=df_daily['close']).average_true_range().iloc[-1]

            curr_rsi, prev_rsi = rsi_ser.iloc[-1], rsi_ser.iloc[-2]
            curr_macd, prev_macd, curr_sig = macd_line.iloc[-1], macd_line.iloc[-2], signal_line.iloc[-1]
            curr_w_rsi, prev_w_rsi = get_weekly_rsi_resampled(df_daily)
            if curr_w_rsi is None: continue

            direction = 'LONG' if curr_rsi <= 30 else ('SHORT' if curr_rsi >= 70 else None)
            existing = watchlist_map.get(symbol)

            if direction or existing:
                final_dir = direction if direction else existing['direction']
                ext_price = existing['extreme_price'] if existing else df_daily['close'].iloc[-1]

                if final_dir == 'LONG':
                    ext_price = min(df_daily['low'].iloc[-1], ext_price)
                else:
                    ext_price = max(df_daily['high'].iloc[-1], ext_price)

                # --- 14-DAY EARNINGS CHECK ---
                report_date_str = earnings_map.get(symbol)
                earnings_safe = True
                if report_date_str:
                    report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
                    days_to = (report_date - datetime.now().date()).days
                    if 0 <= days_to <= 14: earnings_safe = False

                # --- MOMENTUM ALIGNMENT (Corrected Short Logic) ---
                if final_dir == 'LONG':
                    rsi_align = curr_rsi > prev_rsi
                    w_rsi_align = curr_w_rsi > prev_w_rsi
                    macd_slope = curr_macd > prev_macd
                else:  # SHORT
                    rsi_align = curr_rsi < prev_rsi
                    w_rsi_align = curr_w_rsi < prev_w_rsi
                    macd_slope = curr_macd < prev_macd

                is_ready = all([rsi_align, w_rsi_align, macd_slope, earnings_safe])
                macd_cross = curr_macd > curr_sig if final_dir == 'LONG' else curr_macd < curr_sig

                bulk_results.append({
                    "symbol": symbol, "direction": final_dir, "rsi_touch_value": float(curr_rsi),
                    "extreme_price": float(ext_price), "atr": float(atr_val), "is_ready": is_ready,
                    "next_earnings": report_date_str,
                    "logic_trail": {
                        "d_rsi": float(curr_rsi), "w_rsi": float(curr_w_rsi),
                        "macd_ready": bool(macd_slope), "macd_cross": bool(macd_cross)
                    },
                    "last_updated": datetime.now().isoformat()
                })

                if len(bulk_results) >= 100:
                    try:
                        supabase.table("signal_watchlist").upsert(bulk_results,
                                                                  on_conflict="symbol,direction").execute()
                        bulk_results = []
                    except Exception as e:
                        logger.error(f"❌ Batch upsert failed: {e}")

        except Exception as e:
            logger.error(f"❌ Error scanning {symbol}: {e}")

    if bulk_results:
        try:
            supabase.table("signal_watchlist").upsert(bulk_results, on_conflict="symbol,direction").execute()
        except Exception as e:
            logger.error(f"❌ Final cleanup upsert failed: {e}")

    logger.info("🏁 Final scanner run complete.")


if __name__ == "__main__":
    run_sidbot_scanner()