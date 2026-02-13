import time
import requests
import io
import pandas as pd
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from supabase import create_client

# --- 1. CONFIGURATION ---
load_dotenv()
ALPACA_KEY = os.getenv('APCA_API_KEY_ID')
ALPACA_SECRET = os.getenv('APCA_API_SECRET_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY')

stock_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
crypto_client = CryptoHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# --- 2. TICKER UNIVERSE ---
from get_all_tickers import get_tickers as gt


def get_ticker_universe():
    print("🚀 Fetching tickers from exchange lists...")
    try:
        # This fetches directly from NYSE/NASDAQ/AMEX lists
        # It's much more stable than the iShares CSV
        tickers = gt.get_tickers()

        # Filter: Only stocks (no warrants/preferred), 5 chars or less, all letters
        equities = [str(t).strip() for t in tickers
                    if len(str(t)) <= 5 and str(t).isalpha()]

        # Limit to ~1000 to keep your storage manageable as planned
        equities = equities[:1000]

        print(f"✅ Success! Retrieved {len(equities)} tickers.")
    except Exception as e:
        print(f"⚠️ Exchange fetch failed ({e}). Using fallback.")
        equities = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA']

    forex = ['EUR/USD', 'USD/JPY', 'GBP/USD', 'AUD/USD', 'USD/CAD', 'USD/CHF', 'NZD/USD',
             'EUR/JPY', 'GBP/JPY', 'GBP/NZD', 'EUR/NZD', 'CHF/JPY', 'GBP/AUD', 'GBP/CAD',
             'GBP/CHF', 'NZD/JPY', 'EUR/CAD', 'CAD/JPY', 'AUD/NZD', 'AUD/JPY', 'NZD/CHF',
             'EUR/AUD', 'AUD/CAD', 'NZD/CAD', 'EUR/CHF', 'AUD/CHF', 'CAD/CHF']

    crypto = ['BTC/USD', 'ETH/USD']
    return {"EQUITY": equities, "FOREX": forex, "CRYPTO": crypto}
# --- 3. CORE FETCHER ---
def populate_lane(symbols, timeframe_obj, timeframe_label, days_back, asset_class):
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
                    # We specify the constraint columns exactly to fix the 409 error
                    supabase.table("market_data").upsert(
                        records[j:j + 1000],
                        on_conflict="symbol,timestamp,timeframe"
                    ).execute()
                print(f"✅ {asset_class} | {timeframe_label} | Batch {i // 50 + 1} uploaded.")

            time.sleep(0.5)
        except Exception as e:
            print(f"❌ Error on {asset_class} batch {batch[0]}: {e}")


# --- 4. EXECUTION ---
if __name__ == "__main__":
    universe = get_ticker_universe()

    targets = [
        {"label": "1Day", "tf": TimeFrame.Day, "days": 3285},
        {"label": "4Hour", "tf": TimeFrame(4, TimeFrameUnit.Hour), "days": 730},
        {"label": "1Hour", "tf": TimeFrame.Hour, "days": 365},
        {"label": "15Min", "tf": TimeFrame(15, TimeFrameUnit.Minute), "days": 180}
    ]

    for target in targets:
        for a_class, s_list in universe.items():
            populate_lane(s_list, target["tf"], target["label"], target["days"], a_class)