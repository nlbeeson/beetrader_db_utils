import os
import pandas as pd
from dotenv import load_dotenv
from populate_db import get_clients  # Reuse your existing connection logic

load_dotenv()
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")


def update_global_earnings():
    clients = get_clients()
    supabase = clients['supabase_client']

    print("🔍 Fetching Master Earnings Calendar from Alpha Vantage...")
    # 1. Fetch full calendar (Alpha Vantage returns ~3 months of data)
    url = f'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&horizon=3month&apikey={ALPHAVANTAGE_API_KEY}'

    try:
        df = pd.read_csv(url)
        # 2. Get the list of tickers you actually track
        meta_resp = supabase.table("ticker_reference").select("symbol").execute()
        my_symbols = [item['symbol'] for item in meta_resp.data]

        # 3. Filter and Format
        # Alpha Vantage might have different symbol formats or missing some
        found_df = df[df['symbol'].isin(my_symbols)]
        records = []
        for _, row in found_df.iterrows():
            records.append({
                "symbol": row['symbol'],
                "report_date": row['reportDate']
            })

        # 4. Upsert to Supabase
        if records:
            # Batch upsert to handle potential large lists
            for i in range(0, len(records), 100):
                supabase.table("earnings_calendar").upsert(records[i:i + 100]).execute()
            print(f"✅ Synced earnings for {len(records)} tickers.")
        
        # 5. Identify missing symbols (N/A fix)
        found_symbols = set(found_df['symbol'].tolist())
        missing_symbols = [s for s in my_symbols if s not in found_symbols]
        if missing_symbols:
            print(f"⚠️ {len(missing_symbols)} symbols missing from current 3-month calendar.")
            # In a future update, we could iterate through missing symbols and fetch individually if needed
            # but Alpha Vantage free tier has strict limits.

    except Exception as e:
        print(f"❌ Earnings Sync Failed: {e}")


if __name__ == "__main__":
    update_global_earnings()