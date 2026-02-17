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
    handlers=[logging.FileHandler("daily_update.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def run_daily_update():
    load_dotenv()
    clients = get_clients()
    alpaca = clients['alpaca_client']
    supabase = clients['supabase_client']

    # 1. GET SYMBOLS
    # We pull from ticker_metadata so it always matches what you intend to track
    ticker_resp = supabase.table("ticker_reference").select("symbol").execute()
    symbols = [item['symbol'] for item in ticker_resp.data]

    if not symbols:
        logger.error("No symbols found in ticker_metadata. Aborting.")
        return

    # 2. DEFINE WINDOW
    # We fetch the last 5 days to cover weekends and any late-day adjustments
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5)

    logger.info(f"🚀 Updating {len(symbols)} symbols from {start_date.date()} to {end_date.date()}...")

    # 3. FETCH & UPSERT IN BATCHES
    for i in range(0, len(symbols), 100):
        batch = symbols[i:i + 100]
        try:
            request_params = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame.Day,
                start=start_date,
                end=end_date,
                adjustment='split'
            )

            bars = alpaca.get_stock_bars(request_params)
            df = bars.df

            if df.empty:
                continue

            df = df.reset_index()

            # Ensure numeric types and format for Supabase
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['timestamp'] = df['timestamp'].apply(lambda x: x.isoformat())
            df['timeframe'] = "1Day"
            df['asset_class'] = "US_EQUITY"
            df['source'] = "alpaca"

            update_data = df[[
                'symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'timeframe', 'asset_class', 'source'
            ]].to_dict('records')

            if update_data:
                # Upsert ensures no duplicates on (symbol, timeframe, timestamp)
                supabase.table("market_data").upsert(
                    update_data,
                    on_conflict="symbol,timeframe,timestamp"
                ).execute()

        except Exception as e:
            logger.error(f"❌ Error updating batch starting with {batch[0]}: {e}")

    logger.info("🏁 Daily update complete.")


if __name__ == "__main__":
    run_daily_update()