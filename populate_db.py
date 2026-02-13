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


def get_tickers_from_ishares_xml(file_path):
    """Parses the iShares Russell 1000 Growth XML for tickers."""
    try:
        df = pd.read_excel(file_path, sheet_name='Holdings', skiprows=9)
        tickers = df['Ticker'].dropna().unique().tolist()
        return [t for t in tickers if len(str(t)) <= 5]
    except Exception as e:
        logger.error(f"Error parsing iShares XML: {e}")
        return []


def get_additional_tickers(file_path):
    """Scans a secondary file (CSV or TXT) for additional custom tickers."""
    if not os.path.exists(file_path):
        logger.warning(f"Additional tickers file not found at {file_path}")
        return []

    try:
        # Assumes a simple CSV with a 'symbol' column or a plain text file with one ticker per line
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
            return df['symbol'].dropna().unique().tolist()
        else:
            with open(file_path, 'r') as f:
                return [line.strip().upper() for line in f if line.strip()]
    except Exception as e:
        logger.error(f"Error reading additional tickers: {e}")
        return []


def sync_ticker_metadata(symbols):
    """Updates the ticker_metadata table with the combined list of symbols."""
    supabase = get_clients()['supabase_client']
    records = [{"symbol": s, "asset_class": "US_EQUITY", "source": "combined_import"} for s in symbols]

    logger.info(f"🔄 Syncing {len(symbols)} symbols to metadata...")
    supabase.table("ticker_metadata").upsert(records, on_conflict="symbol").execute()


def populate_market_data():
    clients = get_clients()
    supabase = clients['supabase_client']
    alpaca = clients['alpaca_client']

    # 1. COMBINE TICKER LISTS
    # Replace these paths with your actual filenames in ticker_imports/
    main_file = "ticker_imports/iShares-Russell-1000-Growth-ETF_fund.xml"
    extra_file = "ticker_imports/manual_watchlist.csv"

    symbols = get_tickers_from_ishares_xml(main_file)
    extra_symbols = get_additional_tickers(extra_file)

    # Merge and remove duplicates
    combined_symbols = list(set(symbols + extra_symbols))

    # 2. Sync metadata first to ensure the 'map' is correct
    sync_ticker_metadata(combined_symbols)

    # 3. Process lanes (Daily and Hourly)
    tf_configs = [
        {"tf": TimeFrame.Day, "label": "1Day", "days": 1000, "batch": 50},
        {"tf": TimeFrame.Hour, "label": "1Hour", "days": 365, "batch": 10},
    ]

    for config in tf_configs:
        label = config["label"]
        logger.info(f"🚀 Processing {label} lane...")

        for i in range(0, len(combined_symbols), config["batch"]):
            batch = combined_symbols[i:i + config["batch"]]
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
                df['timestamp'] = df['timestamp'].apply(lambda x: x.isoformat())
                df['timeframe'] = label
                df['asset_class'] = "US_EQUITY"
                df['source'] = "alpaca"

                records = df[['symbol', 'timestamp', 'open', 'high', 'low', 'close',
                              'volume', 'timeframe', 'asset_class', 'source']].to_dict('records')

                if records:
                    supabase.table("market_data").upsert(records, on_conflict="symbol,timestamp,timeframe").execute()
            except Exception as e:
                logger.error(f"❌ Error in batch {batch}: {e}")


if __name__ == '__main__':
    populate_market_data()