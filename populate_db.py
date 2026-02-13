import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# --- CONFIG ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
ALPACA_KEY = os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET = os.getenv("APCA_API_SECRET_KEY")

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_clients():
    return {
        "supabase_client": create_client(SUPABASE_URL, SUPABASE_KEY),
        "alpaca_client": StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
    }


def populate_market_data():
    clients = get_clients()
    supabase = clients['supabase_client']
    alpaca = clients['alpaca_client']

    # 1. Fetch the 1,500 symbols we already have in ticker_metadata
    tickers_resp = supabase.table("ticker_metadata").select("symbol").execute()
    symbols = [t['symbol'] for t in tickers_resp.data]

    # 2. Config with dynamic batching to avoid timeouts
    tf_configs = [
        {"tf": TimeFrame.Day, "label": "1Day", "days": 1000, "batch": 50},
        {"tf": TimeFrame.Hour, "label": "1Hour", "days": 365, "batch": 10},  # Reduced to 10 symbols
    ]

    for config in tf_configs:
        label = config["label"]
        logger.info(f"🚀 Processing {label} lane...")

        for i in range(0, len(symbols), config["batch"]):
            batch = symbols[i:i + config["batch"]]
            request_params = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=config["tf"],
                start=datetime.now() - timedelta(days=config["days"]),
                adjustment='all'
            )

            try:
                bars = alpaca.get_stock_bars(request_params)
                if bars.df.empty: continue

                df = bars.df.reset_index()

                # Format for Supabase
                df['timestamp'] = df['timestamp'].apply(lambda x: x.isoformat())
                df['timeframe'] = label
                df['asset_class'] = "US_EQUITY"
                df['source'] = "alpaca"

                records = df[[
                    'symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'timeframe', 'asset_class', 'source'
                ]].to_dict('records')

                if records:
                    # Upsert records
                    supabase.table("market_data").upsert(
                        records,
                        on_conflict="symbol,timestamp,timeframe"
                    ).execute()

            except Exception as e:
                logger.error(f"❌ Error in batch {batch}: {e}")


if __name__ == '__main__':
    populate_market_data()