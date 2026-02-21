import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from supabase import create_client
from datetime import datetime, timezone,timedelta
from dotenv import load_dotenv
import os
import json

load_dotenv()
# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
ALPACA_KEY = os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET = os.getenv("APCA_API_SECRET_KEY")

db = create_client(SUPABASE_URL, SUPABASE_KEY)
alpaca = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)

TIMEFRAME_MAP = {
    '1d': TimeFrame.Day,
    '1h': TimeFrame.Hour,
    '4h': TimeFrame(4, TimeFrameUnit.Hour),
    '15m': TimeFrame(15, TimeFrameUnit.Minute)
}


def catchup():
    # Fix 1: SIP DATA DELAY
    # Stay 16 minutes behind current time to satisfy Alpaca Free Tier restrictions
    safe_now = datetime.now(timezone.utc) - timedelta(minutes=16)

    for tf_str, alpaca_tf in TIMEFRAME_MAP.items():
        print(f"\n--- Processing Timeframe: {tf_str} ---")

        # Call the high-performance RPC we created to find the gaps
        res = db.rpc('get_symbol_stats', {'target_timeframe': tf_str}).execute()
        if not res.data:
            print(f"No symbols found for {tf_str}")
            continue

        for row in res.data:
            symbol = row['symbol']
            # Fix 2: NUDGE START DATE
            # Add 1 minute to last_ts to avoid inclusive duplicate at the boundary
            last_ts = pd.to_datetime(row['latest_record'])
            request_start = last_ts + timedelta(minutes=1)

            # Only request if the gap is larger than the timeframe period
            if (safe_now - request_start).total_seconds() > 60:
                print(
                    f"[{tf_str}] {symbol}: {request_start.strftime('%Y-%m-%d %H:%M')} -> {safe_now.strftime('%H:%M')}")

                try:
                    request_params = StockBarsRequest(
                        symbol_or_symbols=symbol,
                        timeframe=alpaca_tf,
                        start=request_start,
                        end=safe_now
                    )
                    bars_data = alpaca.get_stock_bars(request_params)

                    if symbol in bars_data.data and bars_data.data[symbol]:
                        # Handle DataFrame extraction (Alpaca-py quirk)
                        bars = bars_data.df.xs(symbol) if len(bars_data.data) > 1 else bars_data.df
                        bars = bars.reset_index()

                        # Fix 3: NOT-NULL CONSTRAINTS
                        # Database requires asset_class and source
                        bars['symbol'] = symbol
                        bars['timeframe'] = tf_str
                        bars['asset_class'] = 'US_EQUITY'
                        bars['source'] = 'alpaca'

                        # Fix 4: INTEGER SYNTAX ERROR
                        # Cast floats like 94465.0 to 94465
                        for col in ['volume', 'trade_count']:
                            if col in bars.columns:
                                bars[col] = bars[col].fillna(0).astype(int)

                        # Fix 5: TIMESTAMP SERIALIZATION
                        # Convert Pandas objects to strings for the Supabase API
                        for col in bars.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
                            bars[col] = bars[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                        # Convert back to list of dicts for upload
                        data = bars.to_dict(orient='records')

                        # Fix 6: EXPLICIT UPSERT CONFLICT
                        # Prevents the "23505 Duplicate Key" error from skipping the ticker
                        db.table('market_data').upsert(
                            data,
                            on_conflict='symbol,timestamp,timeframe'
                        ).execute()

                        print(f"   + Successfully synced {len(data)} rows.")
                    else:
                        print(f"   - No new bars available for {symbol}")

                except Exception as e:
                    # Skip problematic symbols (Delisted, IPOs, etc.) so sync continues
                    print(f"   ! Error updating {symbol}: {e}")

if __name__ == "__main__":
    catchup()