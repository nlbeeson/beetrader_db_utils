import os
import time
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
from utils import alpaca, get_symbols

# 1. Load your credentials
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_KEY")

# 2. Initialize the Web Client (Port 443)
if url and key:
    supabase = create_client(url, key)
else:
    supabase = None

def run_sync():
    if not supabase:
        print("Supabase credentials missing. Please check your .env file.")
        return
    
    try:
        symbols_to_sync = get_symbols()
    except Exception as e:
        print(f"Error getting symbols: {e}")
        return
    print(f"Starting sync for {len(symbols_to_sync)} symbols...")
    
    # Syncing daily bars for today
    for idx, symbol in enumerate(symbols_to_sync, 1):
        try:
            # Fetch latest daily bar
            df = alpaca.get_bars(symbol, '1Day', limit=1, adjustment='all').df
            if df.empty:
                continue
                
            last_bar = df.iloc[-1]
            clean_ts = df.index[-1].isoformat()
            
            payload = {
                "symbol": symbol.replace('/', ''),
                "timestamp": clean_ts,
                "open": float(last_bar['open']),
                "high": float(last_bar['high']),
                "low": float(last_bar['low']),
                "close": float(last_bar['close']),
                "volume": int(last_bar['volume']),
                "vwap": float(last_bar.get('vw', last_bar['close'])),
                "timeframe": '1Day'
            }

            supabase.table("market_data").upsert(
                payload,
                on_conflict="symbol,timestamp,timeframe",
                ignore_duplicates=True
            ).execute()
            
            if idx % 100 == 0:
                print(f"Synced {idx}/{len(symbols_to_sync)}...")
                
        except Exception as e:
            print(f"Error syncing {symbol}: {e}")

if __name__ == "__main__":
    run_sync()