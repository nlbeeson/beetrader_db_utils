import os
import logging
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# --- CONFIG ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
ALPACA_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY")

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_clients():
    return {
        "supabase_client": create_client(SUPABASE_URL, SUPABASE_KEY),
        "alpaca_client": StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
    }


def sync_sector_metadata(supabase):
    """Parses Russell 2000 XML and syncs unique sectors to ticker_metadata."""
    logger.info("📂 Scanning for latest iShares import...")
    xml_path = "ticker_imports/russell_2000.xml"

    if not os.path.exists(xml_path):
        logger.warning(f"No XML found at {xml_path}. Skipping sector sync.")
        return

    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        raw_metadata = []
        # Adjusted for standard iShares XML structure
        for row in root.findall('.//Table'):
            symbol = row.find('Ticker').text if row.find('Ticker') is not None else None
            sector = row.find('Sector').text if row.find('Sector') is not None else "Unknown"

            if symbol and len(symbol) <= 5:  # Filter out weird non-stock entries
                raw_metadata.append({"symbol": symbol, "sector": sector})

        # DEDUPLICATION: Fixes ERROR 21000
        # This keeps only the last occurrence of a symbol in the list
        unique_meta = {item['symbol']: item for item in raw_metadata}.values()
        final_list = list(unique_meta)

        logger.info(f"💾 Syncing Sector metadata for {len(final_list)} unique symbols...")

        # Batch upload to avoid timeout
        for i in range(0, len(final_list), 100):
            batch = final_list[i:i + 100]
            supabase.table("ticker_metadata").upsert(batch, on_conflict="symbol").execute()

    except Exception as e:
        logger.error(f"❌ XML Parsing/Sync Error: {e}")


def populate_market_data():
    """Pulls historical data and handles the 17M+ row table."""
    clients = get_clients()
    supabase = clients['supabase_client']
    alpaca = clients['alpaca_client']

    # 1. Update Sectors First
    sync_sector_metadata(supabase)

    # 2. Get the symbol list from our metadata
    tickers_resp = supabase.table("ticker_metadata").select("symbol").execute()
    symbols = [t['symbol'] for t in tickers_resp.data]

    # 3. Timeframes to populate
    # We focus on Day first, then move to lower timeframes
    timeframes = [TimeFrame.Day, TimeFrame.Hour]

    for tf in timeframes:
        logger.info(f"🚀 Populating {tf} data...")
        # Alpaca allows batching symbols to reduce API calls
        for i in range(0, len(symbols), 50):
            batch = symbols[i:i + 50]
            request_params = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=tf,
                start=datetime(2023, 1, 1),  # ~3 years of history
                adjustment='all'
            )

            try:
                bars = alpaca.get_stock_bars(request_params)
                if not bars.df.empty:
                    df = bars.df.reset_index()

                    records = []
                    for _, row in df.iterrows():
                        records.append({
                            "symbol": row['symbol'],
                            "timestamp": row['timestamp'].isoformat(),
                            "open": float(row['open']),
                            "high": float(row['high']),
                            "low": float(row['low']),
                            "close": float(row['close']),
                            "volume": int(row['volume']),
                            "timeframe": str