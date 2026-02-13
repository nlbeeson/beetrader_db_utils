import time
import logging
import pandas as pd
import os
import glob
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from supabase import create_client

# --- 0. LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("beetrader_db.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# --- 1. CONFIGURATION ---
def get_clients():
    load_dotenv()
    ALPACA_KEY = os.getenv('APCA_API_KEY_ID')
    ALPACA_SECRET = os.getenv('APCA_API_SECRET_KEY')
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY')

    stock_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
    crypto_client = CryptoHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
    supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)

    return {
        "stock_client": stock_client,
        "crypto_client": crypto_client,
        "supabase_client": supabase_client
    }


def get_ticker_universe(supabase):
    logger.info("📂 Scanning for latest iShares import...")
    equities = []
    metadata = []

    import_files = glob.glob('ticker_imports/*.xml')
    if not import_files:
        logger.warning("⚠️ No XML found. Using Russell CSV fallback.")
        df = pd.read_csv('russell_2000_components.csv')
        return {"EQUITY": df['Ticker'].tolist()}

    latest_file = max(import_files, key=os.path.getctime)
    logger.info(f"📄 Processing: {latest_file}")

    try:
        tree = ET.parse(latest_file)
        root = tree.getroot()
        ns = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}

        # Identify Column Indices dynamically
        header_row = None
        for row in root.findall('.//ss:Row', ns):
            cells = row.findall('ss:Cell', ns)
            values = [c.find('ss:Data', ns).text if c.find('ss:Data', ns) is not None else '' for c in cells]
            if 'Ticker' in values and 'Sector' in values:
                header_row = values
                break

        t_idx = header_row.index('Ticker')
        s_idx = header_row.index('Sector')

        for row in root.findall('.//ss:Row', ns):
            cells = row.findall('ss:Cell', ns)
            if len(cells) <= max(t_idx, s_idx): continue

            t_data = cells[t_idx].find('ss:Data', ns)
            s_data = cells[s_idx].find('ss:Data', ns)

            if t_data is not None:
                ticker = str(t_data.text).strip()
                sector = str(s_data.text).strip() if s_data is not None else 'Unknown'

                if len(ticker) <= 5 and ticker.isalpha() and ticker != 'Ticker':
                    equities.append(ticker)
                    metadata.append({"symbol": ticker, "sector": sector})

        # Save metadata so the scanner knows which Sector ETF to check
        if metadata:
            logger.info(f"💾 Syncing Sector metadata for {len(metadata)} symbols...")
            for i in range(0, len(metadata), 500):
                supabase.table("ticker_metadata").upsert(metadata[i:i + 500], on_conflict="symbol").execute()

    except Exception as e:
        logger.error(f"❌ XML Error: {e}")

    return {
        "EQUITY": list(set(equities)),
        "FOREX": ['EUR/USD', 'USD/JPY', 'GBP/USD'],
        "CRYPTO": ['BTC/USD', 'ETH/USD']
    }


def populate_lane(symbols, timeframe_obj, timeframe_label, days_back, asset_class, clients):
    supabase = clients['supabase_client']
    stock_client = clients['stock_client']
    crypto_client = clients['crypto_client']

    start_date = datetime.now() - timedelta(days=days_back)
    is_alt = asset_class in ['FOREX', 'CRYPTO']
    client = crypto_client if is_alt else stock_client

    for i in range(0, len(symbols), 50):
        batch = symbols[i:i + 50]
        try:
            if is_alt:
                req = CryptoBarsRequest(symbol_or_symbols=batch, timeframe=timeframe_obj, start=start_date)
                bars = client.get_crypto_bars(req)
            else:
                req = StockBarsRequest(symbol_or_symbols=batch, timeframe=timeframe_obj, start=start_date,
                                       adjustment='all')
                bars = client.get_stock_bars(req)

            records = []
            if bars.data:
                for symbol, bar_list in bars.data.items():
                    # SIDBOT RULE: Filter out stocks < $5
                    if not is_alt and bar_list:
                        if bar_list[-1].close < 5.00:
                            continue

                    for b in bar_list:
                        records.append({
                            "symbol": symbol.replace('/', ''),
                            "asset_class": asset_class,
                            "timestamp": b.timestamp.isoformat(),
                            "open": b.open, "high": b.high, "low": b.low, "close": b.close,
                            "volume": b.volume, "vwap": b.vwap, "timeframe": timeframe_label,
                            "source": "ALPACA"
                        })

            if records:
                for j in range(0, len(records), 1000):
                    supabase.table("market_data").upsert(
                        records[j:j + 1000],
                        on_conflict="symbol,timestamp,timeframe"
                    ).execute()

            time.sleep(0.5)
        except Exception as e:
            logger.error(f"❌ Error on {asset_class} batch starting with {batch[0]}: {e}")


if __name__ == "__main__":
    c = get_clients()
    u = get_ticker_universe(c['supabase_client'])

    targets = [
        {"label": "1Day", "tf": TimeFrame.Day, "days": 3285},
        {"label": "4Hour", "tf": TimeFrame(4, TimeFrameUnit.Hour), "days": 730},
        {"label": "1Hour", "tf": TimeFrame.Hour, "days": 365},
        {"label": "15Min", "tf": TimeFrame(15, TimeFrameUnit.Minute), "days": 180}
    ]

    for target in targets:
        for a_class, s_list in u.items():
            populate_lane(s_list, target["tf"], target["label"], target["days"], a_class, c)