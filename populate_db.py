import time
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
    
    return stock_client, crypto_client, supabase_client

# --- 2. TICKER UNIVERSE ---

def get_ticker_universe():
    print("📂 Scanning for latest iShares import...")
    equities = []

    # 1. Automatically find the most recent XML file in your folder
    import_files = glob.glob('ticker_imports/*.xml')
    if not import_files:
        print("⚠️ No XML file found in ticker_imports/. Using fallback.")
        return {"EQUITY": ['AAPL', 'MSFT', 'TSLA'], "FOREX": [], "CRYPTO": []}

    latest_file = max(import_files, key=os.path.getctime)
    print(f"📄 Processing: {latest_file}")

    try:
        tree = ET.parse(latest_file)
        root = tree.getroot()
        ns = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}

        for row in root.findall('.//ss:Row', ns):
            cells = row.findall('ss:Cell', ns)
            if not cells: continue

            # The Ticker is in the first cell of the row
            ticker_data = cells[0].find('ss:Data', ns)
            if ticker_data is not None:
                t = str(ticker_data.text).strip()
                # Validation: Standard equity tickers only
                if len(t) <= 5 and t.isalpha() and t != 'Ticker':
                    equities.append(t)

        print(f"✅ Extracted {len(equities)} tickers from iShares XML.")
    except Exception as e:
        print(f"❌ Failed to parse XML: {e}")

    # Deduplicate and return
    return {
        "EQUITY": list(set(equities)),
        "FOREX": ['EUR/USD', 'USD/JPY', 'GBP/USD'],
        "CRYPTO": ['BTC/USD', 'ETH/USD']
    }

# --- 3. CORE FETCHER ---
def populate_lane(symbols, timeframe_obj, timeframe_label, days_back, asset_class, clients=None):
    if clients is None:
        stock_client, crypto_client, supabase = get_clients()
    else:
        stock_client, crypto_client, supabase = clients

    start_date = datetime.now() - timedelta(days=days_back)
    is_alt = asset_class in ['FOREX', 'CRYPTO']
    client = crypto_client if is_alt else stock_client

    # Process in batches of 50 to respect API limits
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
                # Chunk the upload to Supabase (1000 rows at a time)
                for j in range(0, len(records), 1000):
                    supabase.table("market_data").upsert(
                        records[j:j + 1000],
                        on_conflict="symbol,timestamp,timeframe"
                    ).execute()
                print(f"✅ {asset_class} | {timeframe_label} | Batch {i // 50 + 1} ({batch[0]}...) uploaded.")

            time.sleep(0.5)  # Rate limit safety
        except Exception as e:
            print(f"❌ Error on {asset_class} batch starting with {batch[0]}: {e}")


# --- 4. EXECUTION ---
if __name__ == "__main__":
    clients = get_clients()
    universe = get_ticker_universe()

    targets = [
        {"label": "1Day", "tf": TimeFrame.Day, "days": 3285},
        {"label": "4Hour", "tf": TimeFrame(4, TimeFrameUnit.Hour), "days": 730},
        {"label": "1Hour", "tf": TimeFrame.Hour, "days": 365},
        {"label": "15Min", "tf": TimeFrame(15, TimeFrameUnit.Minute), "days": 180}
    ]

    for target in targets:
        for a_class, s_list in universe.items():
            populate_lane(s_list, target["tf"], target["label"], target["days"], a_class, clients=clients)