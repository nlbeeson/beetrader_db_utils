import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetStatus, AssetClass
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from supabase import create_client

# 1. SETUP CREDENTIALS
load_dotenv()
ALPACA_KEY = os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET = os.getenv("APCA_API_SECRET_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Initialize Clients
trading_client = TradingClient(ALPACA_KEY, ALPACA_SECRET)
data_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_dynamic_symbol_list():
    """Fetches active, tradable, marginable US equities from major exchanges."""
    print("Fetching active assets from Alpaca...")
    search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
    assets = trading_client.get_all_assets(search_params)
    
    # Filter for quality (tradable, marginable, major exchange, no preferred shares)
    symbols = [
        a.symbol for a in assets 
        if a.tradable 
        and a.marginable 
        and a.exchange in ['NYSE', 'NASDAQ']
        and '.' not in a.symbol
    ]
    symbols.sort()
    print(f"Found {len(symbols)} quality symbols.")
    return symbols

def sync_market_data(symbols):
    """Batches symbols to stay under Alpaca rate limits and syncs to Supabase."""
    batch_size = 200  # Alpaca allows up to 200 symbols per request
    total_synced = 0
    
    # We want data for the last available trading day. 
    # For a "daily sync", we can pull the last 2 days to ensure we get the most recent closed bar.
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3)
    
    print(f"Syncing data from {start_date.date()} to {end_date.date()}...")

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        try:
            request_params = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame.Day,
                start=start_date,
                end=end_date,
                adjustment='all'
            )
            bars = data_client.get_stock_bars(request_params)
            
            payloads = []
            # Process returned data
            for symbol, symbol_bars in bars.data.items():
                if not symbol_bars:
                    continue
                
                # We usually want the latest bar
                last_bar = symbol_bars[-1]
                
                payloads.append({
                    "symbol": symbol,
                    "timestamp": last_bar.timestamp.isoformat(),
                    "open": float(last_bar.open),
                    "high": float(last_bar.high),
                    "low": float(last_bar.low),
                    "close": float(last_bar.close),
                    "volume": int(last_bar.volume),
                    "vwap": float(last_bar.vwap) if last_bar.vwap else float(last_bar.close),
                    "timeframe": '1Day'
                })

            if payloads:
                supabase.table("market_data").upsert(
                    payloads,
                    on_conflict="symbol,timestamp,timeframe",
                    ignore_duplicates=True
                ).execute()
                total_synced += len(payloads)
                
            print(f"Processed batch {i//batch_size + 1}: {total_synced}/{len(symbols)} symbols handled.")
            time.sleep(0.1) # Small delay to be nice to APIs

        except Exception as e:
            print(f"Error processing batch starting at {i}: {e}")

if __name__ == "__main__":
    try:
        symbols_to_track = get_dynamic_symbol_list()
        # For testing/safety, you might want to limit this or just run it all
        # symbols_to_track = symbols_to_track[:500] 
        sync_market_data(symbols_to_track)
        print("Sync completed successfully.")
    except Exception as e:
        print(f"Critical error during sync: {e}")
