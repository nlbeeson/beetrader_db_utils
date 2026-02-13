import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from supabase import create_client
from populate_db import get_ticker_universe, populate_lane


# --- 1. CONFIGURATION ---
load_dotenv()
ALPACA_KEY = os.getenv('APCA_API_KEY_ID')
ALPACA_SECRET = os.getenv('APCA_API_SECRET_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY')

# Initialize Clients
stock_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
crypto_client = CryptoHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def run_daily_update():
    universe = get_ticker_universe()  # Reuses your CSV/Watchlist logic

    # For daily updates, we only look back 2-3 days to ensure we catch
    # any "late" reporting or weekend gaps without re-fetching years.
    targets = [
        {"label": "1Day", "tf": TimeFrame.Day, "days": 3},
        {"label": "4Hour", "tf": TimeFrame(4, TimeFrameUnit.Hour), "days": 3},
        {"label": "1Hour", "tf": TimeFrame.Hour, "days": 3},
        {"label": "15Min", "tf": TimeFrame(15, TimeFrameUnit.Minute), "days": 3}
    ]

    for target in targets:
        for a_class, s_list in universe.items():
            # Uses the same populate_lane logic with upsert
            populate_lane(s_list, target["tf"], target["label"], target["days"], a_class)


if __name__ == "__main__":
    run_daily_update()