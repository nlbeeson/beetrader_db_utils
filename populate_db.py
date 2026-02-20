import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
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
DB_CONN = os.getenv("DB_CONNECTION_STRING") or os.getenv("SUPABASE_DB_URL")
ALPACA_KEY = os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET = os.getenv("APCA_API_SECRET_KEY")

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_clients():
    return {
        "supabase_client": create_client(SUPABASE_URL, SUPABASE_KEY),
        "alpaca_client": StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
    }


def get_db_connection():
    """Returns a direct psycopg2 connection to the database."""
    try:
        conn = psycopg2.connect(DB_CONN)
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return None


def bulk_upsert_market_data(data_tuples, timeframe):
    """Targets specific partitioned tables with shortened names."""
    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()

    table_name = f"market_data_{timeframe}"  # Targets market_data_1d, etc.

    # The conflict target MUST match the UNIQUE constraint added above
    query = f"""
        INSERT INTO public.{table_name} 
        (symbol, asset_class, timestamp, open, high, low, close, volume, vwap, trade_count, timeframe, source)
        VALUES %s
        ON CONFLICT (symbol, timestamp, timeframe) DO UPDATE SET
            close = EXCLUDED.close,
            high = GREATEST({table_name}.high, EXCLUDED.high),
            low = LEAST({table_name}.low, EXCLUDED.low),
            volume = {table_name}.volume + EXCLUDED.volume,
            vwap = EXCLUDED.vwap,
            trade_count = EXCLUDED.trade_count;
    """
    try:
        execute_values(cur, query, data_tuples)
        conn.commit()
        logger.info(f"✅ Bulk upserted {len(data_tuples)} records into {table_name}.")
    except Exception as e:
        logger.error(f"❌ Error in bulk_upsert_market_data for {table_name}: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def aggregate_timeframes(symbol, source_tf='15m', target_tf='1h'):
    """Aggregates smaller timeframes into larger ones for the partitioned table."""
    interval_map = {'1h': '1 hour', '4h': '4 hours', '1d': '1 day'}
    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()
    
    query = f"""
        INSERT INTO public.market_data (
            symbol, asset_class, timestamp, open, high, low, close, volume, timeframe, source
        )
        SELECT 
            symbol,
            MAX(asset_class),
            date_trunc('{interval_map[target_tf].split()[1]}', timestamp) as bucket,
            (ARRAY_AGG(open ORDER BY timestamp ASC))[1],
            MAX(high),
            MIN(low),
            (ARRAY_AGG(close ORDER BY timestamp DESC))[1],
            SUM(volume),
            %s,
            MAX(source)
        FROM public.market_data
        WHERE timeframe = %s AND symbol = %s
        GROUP BY symbol, bucket
        ON CONFLICT (symbol, timestamp, timeframe) DO NOTHING;
    """
    try:
        cur.execute(query, (target_tf, source_tf, symbol))
        conn.commit()
        logger.info(f"✅ Aggregated {symbol} from {source_tf} to {target_tf}.")
    except Exception as e:
        logger.error(f"❌ Error aggregating {symbol}: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


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


def get_tickers_from_ishares_xml(file_path):
    import xml.etree.ElementTree as ET
    import os

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return []

    try:
        # Microsoft Excel XML Namespace
        ns = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}
        tree = ET.parse(file_path)
        root = tree.getroot()

        tickers = []
        # 1. Find the 'Holdings' Worksheet
        worksheet = root.find(".//ss:Worksheet[@ss:Name='Holdings']", ns)
        if worksheet is None:
            logger.error("Could not find 'Holdings' worksheet in XML")
            return []

        # 2. Iterate through rows in the Table
        rows = worksheet.findall(".//ss:Row", ns)

        # Row 0-8 are headers/disclaimers. Row 9 is the header 'Ticker'.
        # Data starts at Row 10.
        for row in rows[10:]:
            cells = row.findall("ss:Cell", ns)
            if cells:
                # The Ticker is always in the first cell (index 0)
                data_tag = cells[0].find("ss:Data", ns)
                if data_tag is not None and data_tag.text:
                    ticker = str(data_tag.text).strip()
                    # Filter out non-ticker strings (like disclaimers)
                    if ticker and len(ticker) <= 5 and ticker.isupper():
                        tickers.append(ticker)

        logger.info(f"✅ Successfully extracted {len(tickers)} tickers from iShares XML.")
        return tickers

    except Exception as e:
        logger.error(f"❌ Critical Error parsing iShares SpreadsheetML: {e}")
        return []


def sync_ticker_metadata(symbols):
    """Cleaned metadata sync to match your existing Supabase schema"""
    supabase = get_clients()['supabase_client']
    # Removed 'source' and 'asset_class' since they caused errors
    records = [{"symbol": str(s).strip()} for s in symbols if len(str(s)) <= 5]

    logger.info(f"🔄 Syncing {len(records)} symbols to metadata...")
    if records:
        # This uses the simplified schema we confirmed in your last error log
        supabase.table("ticker_reference").upsert(records, on_conflict="symbol").execute()

def populate_market_data():
    import glob
    clients = get_clients()
    supabase = clients['supabase_client']
    alpaca = clients['alpaca_client']

    # 1. DYNAMIC FILE DISCOVERY
    ishares_files = glob.glob("ticker_imports/iShares*.xml")
    main_file = ishares_files[0] if ishares_files else None
    extra_file = "ticker_imports/manual_watchlist.csv"

    symbols = []
    if main_file:
        logger.info(f"📂 Found iShares file: {main_file}")
        symbols = get_tickers_from_ishares_xml(main_file)
    else:
        logger.error("❌ No iShares XML file found in ticker_imports/")

    extra_symbols = get_additional_tickers(extra_file)

    # FIX: Initialize combined_symbols outside of conditional blocks
    combined_symbols = list(set(symbols + extra_symbols))

    if not combined_symbols:
        logger.error("🚫 No symbols found to process. Exiting.")
        return

    # 2. Sync metadata first
    sync_ticker_metadata(combined_symbols)

    # 3. Process lanes (15m, 1h, 1d)
    tf_configs = [
        {"tf": TimeFrame.Minute * 15, "label": "15m", "days": 30, "batch": 5},
        {"tf": TimeFrame.Hour, "label": "1h", "days": 365, "batch": 10},
        {"tf": TimeFrame.Day, "label": "1d", "days": 1000, "batch": 50},
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
                
                # Format for bulk_upsert_market_data
                df['timestamp'] = df['timestamp'].apply(lambda x: x.isoformat())
                df['timeframe'] = label
                df['asset_class'] = "US_EQUITY"
                df['source'] = "alpaca"
                
                # Alpaca bars may not have vwap/trade_count if they're old
                if 'vwap' not in df.columns: df['vwap'] = None
                if 'trade_count' not in df.columns: df['trade_count'] = None

                # Create tuples for psycopg2 execute_values
                records = [
                    (
                        r['symbol'], r['asset_class'], r['timestamp'],
                        float(r['open']), float(r['high']), float(r['low']), float(r['close']),
                        float(r['volume']), float(r['vwap']) if r['vwap'] is not None else None,
                        int(r['trade_count']) if r['trade_count'] is not None else None,
                        r['timeframe'], r['source']
                    )
                    for _, r in df.iterrows()
                ]

                if records:
                    bulk_upsert_market_data(records)

                # After 15m batch, we could trigger aggregation, but it's more efficient 
                # to do it after all batches are processed or as a separate step.
            except Exception as e:
                logger.error(f"❌ Error in batch {batch}: {e}")

    # Optional: Run aggregation for all symbols after the 15m data is in
    for symbol in combined_symbols:
        aggregate_timeframes(symbol, '15m', '1h')
        aggregate_timeframes(symbol, '1h', '4h')
        aggregate_timeframes(symbol, '1d', '1d') # Re-assert daily if needed, though usually direct.


if __name__ == '__main__':
    populate_market_data()