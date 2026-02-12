import os
import time
import pandas as pd
from supabase import create_client
from utils import alpaca, get_symbols
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_SERVICE_KEY'))

FETCH_TIERS = [('1Day', 9000), ('4Hour', 730), ('1Hour', 365), ('15Min', 180)]


def backfill_data():
    all_symbols = get_symbols()
    for idx, symbol in enumerate(all_symbols, 1):
        print(f"[{idx}/{len(all_symbols)}] {symbol}...")
        for tf, days in FETCH_TIERS:
            start = (pd.Timestamp.now() - pd.Timedelta(days=days)).strftime('%Y-%m-%d')
            try:
                # 1. Fetch data
                df = alpaca.get_bars(symbol, tf, start=start, adjustment='all').df
                if df.empty: continue

                # 2. De-duplicate at the DataFrame level first
                df = df[~df.index.duplicated(keep='last')]

                # 3. Build unique records map
                unique_records = {}
                for ts, row in df.iterrows():
                    clean_ts = ts.isoformat()
                    # VWAP fallback to Close
                    vwap = float(row.get('vw', row['close']))

                    unique_records[(symbol, clean_ts, tf)] = {
                        "symbol": symbol.replace('/', ''),
                        "timestamp": clean_ts,
                        "open": float(row['open']), "high": float(row['high']),
                        "low": float(row['low']), "close": float(row['close']),
                        "volume": int(row['volume']), "vwap": vwap, "timeframe": tf
                    }

                # 4. Modified Batch Upsert
                records = list(unique_records.values())
                for i in range(0, len(records), 500):
                    try:
                        # Use ignore_duplicates=True to skip the 21000 error
                        # This will insert new bars and skip bars that already exist
                        supabase.table("market_data").upsert(
                            records[i:i + 500],
                            on_conflict="symbol,timestamp,timeframe",
                            ignore_duplicates=True  # <--- CRITICAL CHANGE
                        ).execute()
                    except Exception as e:
                        # Log specifically which batch failed to narrow it down
                        print(f"  ! Batch Error at index {i}: {e}")

                time.sleep(0.5)
            except Exception as e:
                print(f"  ! Error {tf}: {e}")


if __name__ == "__main__":
    backfill_data()