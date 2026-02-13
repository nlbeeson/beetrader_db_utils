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
ALPACA_KEY = os.getenv('ALPACA_KEY')
ALPACA_SECRET = os.getenv('ALPACA_SECRET')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

stock_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
crypto_client = CryptoHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# --- 2. TICKER UNIVERSE ---
def get_ticker_universe():
    iwv_url = "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund"
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        res = requests.get(iwv_url, headers=headers)
        # utf-8-sig handles the Byte Order Mark often found in iShares CSVs
        df = pd.read_csv(io.StringIO(res.text), skiprows=9, encoding='utf-8-sig')

        # Clean the tickers: ensure they are strings, 5 chars or less, and all letters
        equities = [str(t).strip() for t in df['Ticker'].dropna().unique()
                    if len(str(t)) <= 5 and str(t).isalpha()]
        print(f"✅ Scraped {len(equities)} equities from Russell 3000.")
    except Exception as e:
        print(f"⚠️ Russell 3000 scrape failed ({e}). Using fallback list.")
        equities = ['AAPL', 'MSFT', 'TSLA', 'AMZN', 'GOOGL']

    forex = ['EUR/USD', 'USD/JPY', 'GBP/USD', 'AUD/USD', 'USD/CAD', 'USD/CHF', 'NZD/USD',
             'EUR/JPY', 'GBP/JPY', 'GBP/NZD', 'EUR/NZD', 'CHF/JPY', 'GBP/AUD', 'GBP/CAD',
             'GBP/CHF', 'NZD/JPY', 'EUR/CAD', 'CAD/JPY', 'AUD/NZD', 'AUD/JPY', 'NZD/CHF',
             'EUR/AUD', 'AUD/CAD', 'NZD/CAD', 'EUR/CHF', 'AUD/CHF', 'CAD/CHF']

    crypto = ['BTC/USD', 'ETH/USD']