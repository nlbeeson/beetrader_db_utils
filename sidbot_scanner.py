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
    """Calculates Weekly RSI(14) by resampling daily OHLC data."""
    temp_df = df_daily.copy()
    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
    temp_df.set_index('timestamp', inplace=True)
    df_weekly = temp_df.resample('W-FRI').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
    }).dropna()
    if len(df_weekly) < 15:
        return None, None
    rsi_weekly = RSIIndicator(close=df_weekly['close'], window=14).rsi()
    return rsi_weekly.iloc[-1], rsi_weekly.iloc[-2]


def run_sidbot_scanner():
    clients = get_clients()
    supabase = clients['supabase_client']

    # 1. PRUNE: Hard cutoff at 28 days
    logger.info("Pruning expired signals (28-day window)...")
    cutoff = (datetime.now() - timedelta(days=28)).isoformat()
    supabase.table("signal_watchlist").delete().lt("rsi_touch_date", cutoff).execute()

    # 2. LOAD DATA
    watchlist_resp = supabase.table("signal_watchlist").select("*").execute()
    watchlist_map = {item['symbol']: item for item in watchlist_resp.data}
    earn_resp = supabase.table("earnings_calendar").select("*").execute()
    earnings_map = {item['symbol']: item['report_date'] for item in earn_resp.data}

    # Pull symbols from ticker_metadata instead of market_data to avoid row limits
    tickers_resp = supabase.table("ticker_metadata").select("symbol").execute()
    symbols = [t['symbol'] for t in tickers_resp.data]

    logger.info(f"🔎 Scanning {len(symbols)} symbols from metadata...")
    bulk_results = []

    for symbol in symbols:
        try:
            # Fetch daily data for the symbol
            daily_data = supabase.table("market_data").select("*") \
                .eq("symbol", symbol).eq("timeframe", "1Day") \
                .order("timestamp", desc=True).limit(250).execute()

            # SKIP SYMBOLS WITH INSUFFICIENT HISTORY
            if len(daily_data.data) < 100:
                logger.warning(f"⚠️ Skipping {symbol}: Insufficient history ({len(daily_data.data)} bars).")
                continue

            if len(daily_data.data) < 50:
                continue
            df_daily = pd.DataFrame(daily_data.data).iloc[::-1]

            # Ensure numeric types for indicators
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df_daily.columns:
                    df_daily[col] = pd.to_numeric(df_daily[col], errors='coerce')

            # Indicators
            rsi_ser = RSIIndicator(close=df_daily['close']).rsi()
            macd_obj = MACD(close=df_daily['close'])
            macd_line, signal_line = macd_obj.macd(), macd_obj.macd_signal()
            atr_val = AverageTrueRange(high=df_daily['high'], low=df_daily['low'],
                                       close=df_daily['close']).average_true_range().iloc[-1]

            curr_rsi, prev_rsi = rsi_ser.iloc[-1], rsi_ser.iloc[-2]
            curr_macd, prev_macd, curr_sig = macd_line.iloc[-1], macd_line.iloc[-2], signal_line.iloc[-1]
            curr_w_rsi, prev_w_rsi = get_weekly_rsi_resampled(df_daily)
            if curr_w_rsi is None:
                continue

            # Direction Logic
            direction = 'LONG' if curr_rsi <= 30 else ('SHORT' if curr_rsi >= 70 else None)
            existing = watchlist_map.get(symbol)
            if not direction and existing:
                direction = existing['direction']

            if direction:
                ext_price = existing['extreme_price'] if existing else df_daily['close'].iloc[-1]
                if direction == 'LONG':
                    ext_price = min(df_daily['low'].iloc[-1], ext_price)
                else:
                    ext_price = max(df_daily['high'].iloc[-1], ext_price)

                # Earnings Check
                report_date_str = earnings_map.get(symbol)
                earnings_safe = True
                if report_date_str:
                    days_to = (datetime.strptime(report_date_str, '%Y-%m-%d').date() - datetime.now().date()).days
                    if 0 <= days_to <= 14:
                        earnings_safe = False

                # Corrected Momentum Alignment
                if direction == 'LONG':
                    align = (curr_rsi > prev_rsi) and (curr_w_rsi > prev_w_rsi) and (curr_macd > prev_macd)
                else:  # SHORT
                    align = (curr_rsi < prev_rsi) and (curr_w_rsi < prev_w_rsi) and (curr_macd < prev_macd)

                is_ready = align and earnings_safe
                macd_cross = curr_macd > curr_sig if direction == 'LONG' else curr_macd < curr_sig

                # Maintain original touch date to ensure the 28-day expiry works correctly
                # We use the existing date if available and if the direction hasn't flipped
                existing_date = existing.get('rsi_touch_date') if existing else None
                if existing and existing.get('direction') != direction:
                    existing_date = None  # Reset if signal flipped LONG <-> SHORT

                touch_date = existing_date if existing_date else datetime.now().isoformat()

                bulk_results.append({
                    "symbol": symbol, "direction": direction, "rsi_touch_value": float(curr_rsi),
                    "rsi_touch_date": touch_date,
                    "extreme_price": float(ext_price), "atr": float(atr_val), "is_ready": is_ready,
                    "next_earnings": report_date_str,
                    "logic_trail": {
                        "d_rsi": float(curr_rsi), "w_rsi": float(curr_w_rsi),
                        "macd_ready": bool(align), "macd_cross": bool(macd_cross)
                    },
                    "last_updated": datetime.now().isoformat()
                })

                if len(bulk_results) >= 100:
                    try:
                        supabase.table("signal_watchlist").upsert(bulk_results, on_conflict="symbol").execute()
                        logger.info(f"✅ Synced batch of {len(bulk_results)} symbols")
                        bulk_results = []
                    except Exception as e:
                        logger.error(f"Batch upsert failed: {e}")
        except Exception as e:
            logger.error(f"Error processing symbol {symbol}: {e}")

    if bulk_results:
        try:
            supabase.table("signal_watchlist").upsert(bulk_results, on_conflict="symbol").execute()
            logger.info(f"✅ Final cleanup upsert of {len(bulk_results)} symbols")
        except Exception as e:
            logger.error(f"Final cleanup upsert failed: {e}")
    logger.info("Scanner complete.")


if __name__ == "__main__":
    run_sidbot_scanner()
