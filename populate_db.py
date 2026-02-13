import os
import logging
import pandas as pd
import xml.etree.ElementTree as ET
from lxml import etree
from datetime import datetime, timedelta
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


def get_ticker_universe(supabase=None):
    """Returns a dict of symbols by asset class. Currently focused on US_EQUITY."""
    if not supabase:
        supabase = get_clients()['supabase_client']
    
    resp = supabase.table("ticker_metadata").select("symbol").execute()
    symbols = [item['symbol'] for item in resp.data]
    return {"US_EQUITY": symbols}


def populate_lane(symbols, timeframe, label, days_back, asset_class, clients=None):
    """Generic function to populate a specific timeframe for a list of symbols."""
    if not clients:
        clients = get_clients()
    
    supabase = clients['supabase_client']
    alpaca = clients['alpaca_client']
    
    start_date = datetime.now() - timedelta(days=days_back)
    
    for i in range(0, len(symbols), 50):
        batch = symbols[i:i + 50]
        request_params = StockBarsRequest(
            symbol_or_symbols=batch,
            timeframe=timeframe,
            start=start_date,
            adjustment='all'
        )
        
        try:
            bars = alpaca.get_stock_bars(request_params)
            if bars.df.empty:
                continue
                
            df = bars.df.reset_index()
            
            # Vectorized formatting
            df['timestamp'] = df['timestamp'].apply(lambda x: x.isoformat())
            df['timeframe'] = label
            df['asset_class'] = asset_class
            df['source'] = "alpaca"
            
            # Map columns and convert to list of dicts
            records = df[[
                'symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 
                'timeframe', 'asset_class', 'source'
            ]].to_dict('records')

            if records:
                supabase.table("market_data").upsert(
                    records, 
                    on_conflict="symbol,timestamp,timeframe"
                ).execute()
                
        except Exception as e:
            logger.error(f"❌ Error in lane {label} for batch {i}: {e}")


def sync_sector_metadata(supabase):
    """Parses Russell 2000 XML and syncs unique sectors to ticker_metadata."""
    logger.info("📂 Scanning for latest iShares import...")
    xml_path = "ticker_imports/russell_2000.xml"

    if not os.path.exists(xml_path):
        logger.warning(f"No XML found at {xml_path}. Skipping sector sync.")
        return

    try:
        # Using lxml for better namespace handling
        parser = etree.XMLParser(recover=True)
        tree = etree.parse(xml_path, parser)
        root = tree.getroot()

        # Handle potential namespaces
        ns = root.nsmap
        
        raw_metadata = []
        # Support both namespaced and non-namespaced structures
        xpath_query = './/Table' if not ns else './/{*}Table'
        
        for row in root.findall(xpath_query):
            ticker_node = row.find('Ticker') if not ns else row.find('.//{*}Ticker')
            sector_node = row.find('Sector') if not ns else row.find('.//{*}Sector')
            
            symbol = ticker_node.text if ticker_node is not None else None
            sector = sector_node.text if sector_node is not None else "Unknown"

            if symbol and len(symbol) <= 5:
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
    # Map TimeFrame objects to the labels used in our DB and scanner
    tf_configs = [
        {"tf": TimeFrame.Day, "label": "1Day", "years": 3},
        {"tf": TimeFrame.Hour, "label": "1Hour", "years": 2}
    ]

    for config in tf_configs:
        tf = config["tf"]
        label = config["label"]
        logger.info(f"🚀 Populating {label} data...")
        
        start_date = datetime.now() - timedelta(days=config["years"] * 365)
        
        for i in range(0, len(symbols), 50):
            batch = symbols[i:i + 50]
            request_params = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=tf,
                start=start_date,
                adjustment='all'
            )

            try:
                bars = alpaca.get_stock_bars(request_params)
                if not bars.df.empty:
                    df = bars.df.reset_index()

                    # Vectorized formatting
                    df['timestamp'] = df['timestamp'].apply(lambda x: x.isoformat())
                    df['timeframe'] = label
                    df['asset_class'] = "US_EQUITY"
                    df['source'] = "alpaca"
                    
                    records = df[[
                        'symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume',
                        'timeframe', 'asset_class', 'source'
                    ]].to_dict('records')

                    # Push to Supabase
                    if records:
                        supabase.table("market_data").upsert(
                            records,
                            on_conflict="symbol,timestamp,timeframe"
                        ).execute()

            except Exception as e:
                logger.error(f"Error processing batch {i // 50}: {e}")


if __name__ == "__main__":
    populate_market_data()