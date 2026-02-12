import os
import alpaca_trade_api as tradeapi
from dotenv import load_dotenv

load_dotenv()

APCA_API_KEY_ID = os.getenv('APCA_API_KEY_ID')
APCA_API_SECRET_KEY = os.getenv('APCA_API_SECRET_KEY')
APCA_URL = os.getenv('APCA_URL', 'https://paper-api.alpaca.markets')

alpaca = tradeapi.REST(APCA_API_KEY_ID, APCA_API_SECRET_KEY, APCA_URL)

FOREX_PAIRS = [
    'GBP/NZD', 'GBP/JPY', 'EUR/NZD', 'CHF/JPY', 'GBP/AUD', 'GBP/CAD', 'GBP/CHF', 'NZD/JPY',
    'EUR/CAD', 'CAD/JPY', 'AUD/NZD', 'AUD/JPY', 'USD/CHF', 'NZD/CHF', 'EUR/AUD', 'AUD/CAD',
    'NZD/CAD', 'EUR/CHF', 'AUD/CHF', 'USD/JPY', 'USD/CAD', 'NZD/USD', 'GBP/USD', 'EUR/USD',
    'EUR/JPY', 'CAD/CHF', 'AUD/USD'
]

def get_symbols():
    try:
        # Fetch all active US equities
        active_assets = alpaca.list_assets(status='active', asset_class='us_equity')

        # DEFINITIVE FILTER:
        # Tradable + Marginable (Liquidity) + Fractionable (Market Cap Proxy)
        stocks = [
            a.symbol for a in active_assets
            if a.tradable
               and a.marginable  # Only stocks you can trade on margin
               and a.fractionable  # Generally reserved for larger-cap stocks
               and a.exchange in ['NYSE', 'NASDAQ']
               and '.' not in a.symbol  # Skips preferred shares
               and len(a.symbol) <= 4  # Skips weird warrants/instruments
        ]

        stocks.sort()
        print(f"Refined list to {len(stocks)} high-quality Russell 3000 proxy symbols.")
        return stocks + FOREX_PAIRS
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return FOREX_PAIRS
