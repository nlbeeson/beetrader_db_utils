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
    # We pull from ticker_metadata to ensure all tracked stocks are covered
    logger.info("🔍 Identifying tickers with insufficient history (min 250 bars)...")

    # Fetch all symbols from ticker_metadata
    ticker_resp = supabase.table("ticker_metadata").select("symbol").execute()
    all_target_symbols = [item['symbol'] for item in ticker_resp.data]

    if not all_target_symbols:
        logger.error("No symbols found in ticker_metadata. Aborting.")
        return

    # Fetch counts from market_data
    active_tickers_resp = supabase.table("market_data").select("symbol").eq("timeframe", "1Day").execute()
    
    symbol_counts = {}
    if active_tickers_resp.data:
        all_present_symbols = [item['symbol'] for item in active_tickers_resp.data]
        symbol_counts = pd.Series(all_present_symbols).value_counts().to_dict()

    # Filter for those below the 250-bar stability threshold or missing entirely
    under_seasoned = []
    for symbol in all_target_symbols:
        count = symbol_counts.get(symbol, 0)
        if count < 250:
            under_seasoned.append(symbol)

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