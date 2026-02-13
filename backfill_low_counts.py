import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from populate_db import get_clients

# --- 0. LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("backfill_history.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def run_backfill():
    load_dotenv()
    clients = get_clients()
    alpaca = clients['alpaca_client']
    supabase = clients['supabase_client']

    # 1. IDENTIFY UNDER-SEASONED TICKERS
    # We look for symbols with less than 250 bars in the 1Day timeframe
    logger.info("🔍 Identifying tickers with insufficient history (min 250 bars)...")

    # Fetch all symbols currently in the daily lane
    active_tickers_resp = supabase.table("market_data").select("symbol").eq("timeframe", "1Day").execute()

    if not active_tickers_resp.data:
        logger.error("No data found in market_data. Aborting.")
        return

    # Count occurrences of each symbol
    all_symbols = [item['symbol'] for item in active_tickers_resp.data]
    df_counts = pd.Series(all_symbols).value_counts()

    # Filter for those below the 250-bar stability threshold
    under_seasoned = df_counts[df_counts < 250].index.tolist()

    if not under_seasoned:
        logger.info("✅ All tickers meet the 250-bar requirement.")
        return

    logger.info(f"🚀 Backfilling {len(under_seasoned)} tickers...")

    # 2. DEFINE WINDOW
    # Pulling 2 years of history to guarantee we hit the 250-bar target
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)

    # 3. EXECUTE BACKFILL
    for symbol in under_seasoned:
        try:
            logger.info(f"⏳ Fetching history for {symbol}...")
            request_params = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Day,
                start=start_date,
                end=end_date,
                adjustment='split'
            )

            bars = alpaca.get_stock_bars(request_params)
            df = bars.df

            if df.empty:
                logger.warning(f"⚠️ No data returned for {symbol}")
                continue

            df = df.reset_index()

            # Map columns and format for Supabase
            update_data = []
            for _, row in df.iterrows():
                update_data.append({
                    "symbol": symbol,
                    "timeframe": "1Day",
                    "timestamp": row['timestamp'].isoformat(),
                    "open": float(row['open']),
                    "high": float(row['high']),
                    "low": float(row['low']),
                    "close": float(row['close']),
                    "volume": int(row['volume']),
                    "asset_class": "US_EQUITY",
                    "source": "alpaca"
                })

            if update_data:
                # Upsert ensures we fill holes without creating duplicates
                supabase.table("market_data").upsert(
                    update_data,
                    on_conflict="symbol,timeframe,timestamp"
                ).execute()
                logger.info(f"✅ Successfully backfilled {symbol} ({len(update_data)} bars).")

        except Exception as e:
            logger.error(f"❌ Failed to backfill {symbol}: {e}")

    logger.info("🏁 Backfill process complete.")


if __name__ == "__main__":
    run_backfill()