import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from supabase import create_client
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import os

load_dotenv()
# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
ALPACA_KEY = os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET = os.getenv("APCA_API_SECRET_KEY")

# Alpaca Free Tier Limit (approx 5 years)
TARGET_START = datetime(2021, 2, 21, tzinfo=timezone.utc)

db = create_client(SUPABASE_URL, SUPABASE_KEY)
alpaca = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)


def run_deep_backfill():
    print(f"Starting Deep Backfill to {TARGET_START.strftime('%Y-%m-%d')}...")

    # 1. Fetch the list of symbols starting after our target
    # This uses the RPC we created earlier
    res = db.rpc('get_symbol_stats', {'target_timeframe': '1d'}).execute()

    if not res.data:
        print("No symbols found in database.")
        return

    # Filter for symbols that are stuck at the 2-Year Cap (Feb 2024)
    # We ignore the 27 'Deep History' symbols and recent IPOs
    symbols_to_backfill = [
        row for row in res.data
        if pd.to_datetime(row['earliest_record']) > TARGET_START
           and 'US_EQUITY' in row.get('asset_class', 'US_EQUITY')  # Skip Forex
    ]

    print(f"Found {len(symbols_to_backfill)} symbols to backfill.")

    for row in symbols_to_backfill:
        symbol = row['symbol']
        current_db_start = pd.to_datetime(row['earliest_record'])

        print(f"Backfilling {symbol}: {TARGET_START.date()} -> {current_db_start.date()}")

        try:
            # Request the 'Missing' history window
            request_params = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Day,
                start=TARGET_START,
                end=current_db_start - timedelta(days=1)  # Stop just before current data
            )

            bars_data = alpaca.get_stock_bars(request_params)

            if symbol in bars_data.data and bars_data.data[symbol]:
                bars = bars_data.df.xs(symbol) if len(bars_data.data) > 1 else bars_data.df
                bars = bars.reset_index()

                # Apply the fixes we established in catchup_data.py
                bars['symbol'] = symbol
                bars['timeframe'] = '1d'
                bars['asset_class'] = 'US_EQUITY'
                bars['source'] = 'alpaca'

                for col in ['volume', 'trade_count']:
                    if col in bars.columns:
                        bars[col] = bars[col].fillna(0).astype(int)

                for col in bars.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
                    bars[col] = bars[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                data = bars.to_dict(orient='records')

                # Bulk Upsert
                db.table('market_data').upsert(
                    data,
                    on_conflict='symbol,timestamp,timeframe'
                ).execute()

                print(f"   + Added {len(data)} historical daily bars.")
            else:
                print(f"   - No historical bars found for {symbol} in this range.")

        except Exception as e:
            print(f"   ! Error backfilling {symbol}: {e}")


if __name__ == "__main__":
    run_deep_backfill()