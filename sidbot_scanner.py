import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import AverageTrueRange
from populate_db import get_clients

# --- 0. LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("sidbot_scanner.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Mapping iShares Sector names to State Street SPDR ETFs
SECTOR_MAP = {
    'Information Technology': 'XLK',
    'Financials': 'XLF',
    'Health Care': 'XLV',
    'Energy': 'XLE',
    'Utilities': 'XLU',
    'Consumer Discretionary': 'XLY',
    'Consumer Staples': 'XLP',
    'Industrials': 'XLI',
    'Materials': 'XLB',
    'Real Estate': 'XLRE',
    'Communication': 'XLC'
}


def check_etf_alignment(etf_symbol, direction, supabase):
    """
    Returns True if the ETF's RSI and MACD Line are moving
    in the same direction as the intended trade.
    """
    try:
        # Fetch last 30 days of ETF data
        data = supabase.table("market_data") \
            .select("close") \
            .eq("symbol", etf_symbol) \
            .eq("timeframe", "1Day") \
            .order("timestamp", desc=True) \
            .limit(30).execute()

        if len(data.data) < 26: return False
        df = pd.DataFrame(data.data).iloc[::-1]

        # Calculate momentum
        rsi = RSIIndicator(close=df['close']).rsi()
        macd_line = MACD(close=df['close']).macd()

        rsi_up = rsi.iloc[-1] > rsi.iloc[-2]
        macd_up = macd_line.iloc[-1] > macd_line.iloc[-2]

        if direction == 'LONG':
            return rsi_up and macd_up
        else:  # SHORT
            return (not rsi_up) and (not macd_up)

    except Exception as e:
        logger.error(f"Error checking ETF {etf_symbol}: {e}")
        return False


def calculate_market_context_score(symbol, direction, supabase):
    """
    Calculates a score (0-2) based on SPY and Sector alignment.
    """
    score = 0

    # 1. Broad Market Check (SPY)
    if check_etf_alignment('SPY', direction, supabase):
        score += 1

    # 2. Sector Check
    meta = supabase.table("ticker_metadata").select("sector").eq("symbol", symbol).execute()
    if meta.data:
        sector_name = meta.data[0]['sector']
        etf_ticker = SECTOR_MAP.get(sector_name)
        if etf_ticker and check_etf_alignment(etf_ticker, direction, supabase):
            score += 1

    return score

def get_market_wind_score(symbol_sector, supabase):
    score = 0
    # 1. Check SPY (Market)
    spy_ready = check_etf_alignment('SPY', supabase)
    if spy_ready: score += 1

    # 2. Check Sector ETF
    sector_ticker = SECTOR_MAP.get(symbol_sector)
    if sector_ticker:
        sector_ready = check_etf_alignment(sector_ticker, supabase)
        if sector_ready: score += 1

    return score


def get_weekly_rsi_resampled(df_daily):
    """
    Simulates TradingView's Weekly RSI by resampling daily data to M-F calendar weeks.
    Requires enough daily data to generate 14+ weekly bars.
    """
    temp_df = df_daily.copy()
    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
    temp_df.set_index('timestamp', inplace=True)

    # Resample to Week Ending Friday
    df_weekly = temp_df.resample('W-FRI').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }).dropna()

    # Need at least 15 bars for a stable 14-period RSI
    if len(df_weekly) < 15:
        return None, None

    rsi_weekly = RSIIndicator(close=df_weekly['close'], window=14).rsi()
    return rsi_weekly.iloc[-1], rsi_weekly.iloc[-2]


def run_sidbot_scanner():
    clients = get_clients()
    supabase = clients['supabase_client']

    # 1. PRUNE: Remove signals older than 21 days
    logger.info("🧹 Pruning signals older than 21 days...")
    cutoff = (datetime.now() - timedelta(days=21)).isoformat()
    supabase.table("signal_watchlist").delete().lt("rsi_touch_date", cutoff).execute()

    # 2. FETCH SYMBOLS: Pull unique symbols from market_data
    active_tickers_resp = supabase.table("market_data").select("symbol").eq("timeframe", "1Day").execute()
    symbols = list(set([item['symbol'] for item in active_tickers_resp.data]))

    logger.info(f"🔎 Scanning {len(symbols)} symbols for SidBot criteria...")

    for symbol in symbols:
        try:
            # 3. GET DATA: Fetch 100 days of Daily data (enough for indicators + weekly resample)
            daily_data = supabase.table("market_data") \
                .select("*").eq("symbol", symbol).eq("timeframe", "1Day") \
                .order("timestamp", desc=True).limit(100).execute()

            if len(daily_data.data) < 26: continue
            df_daily = pd.DataFrame(daily_data.data).iloc[::-1]  # Chronological

            # --- CALCULATE DAILY INDICATORS ---
            rsi_daily_ser = RSIIndicator(close=df_daily['close']).rsi()
            macd_obj = MACD(close=df_daily['close'])
            macd_line = macd_obj.macd()
            atr_val = AverageTrueRange(high=df_daily['high'], low=df_daily['low'],
                                       close=df_daily['close']).average_true_range().iloc[-1]

            curr_rsi, prev_rsi = rsi_daily_ser.iloc[-1], rsi_daily_ser.iloc[-2]
            curr_macd, prev_macd = macd_line.iloc[-1], macd_line.iloc[-2]

            # --- CALCULATE WEEKLY RSI ---
            curr_w_rsi, prev_w_rsi = get_weekly_rsi_resampled(df_daily)
            if curr_w_rsi is None: continue

            # --- 4. THE "WAITING ROOM" LOGIC (Initial RSI Touch) ---
            direction = None
            if curr_rsi <= 30:
                direction = 'LONG'
            elif curr_rsi >= 70:
                direction = 'SHORT'

            # Check if symbol is already in watchlist to maintain state
            existing = supabase.table("signal_watchlist").select("*").eq("symbol", symbol).execute()

            if direction or existing.data:
                # Resolve direction (favor new touch, else keep existing)
                final_dir = direction if direction else existing.data[0]['direction']

                # Update Extreme Price (Stop Loss level)
                ext_price = df_daily['close'].iloc[-1]
                if existing.data:
                    old_ext = existing.data[0]['extreme_price']
                    if final_dir == 'LONG':
                        ext_price = min(df_daily['low'].iloc[-1], old_ext if old_ext else 999999)
                    else:
                        ext_price = max(df_daily['high'].iloc[-1], old_ext if old_ext else 0)

                # --- 5. EVALUATE THE HARD RULES ---
                daily_rsi_up = curr_rsi > prev_rsi if final_dir == 'LONG' else curr_rsi < prev_rsi
                weekly_rsi_up = curr_w_rsi > prev_w_rsi if final_dir == 'LONG' else curr_w_rsi < prev_w_rsi
                macd_up = curr_macd > prev_macd if final_dir == 'LONG' else curr_macd < prev_macd

                # Rule 4: Earnings placeholder (Next Step)
                earnings_safe = True

                is_ready = all([daily_rsi_up, weekly_rsi_up, macd_up, earnings_safe])

                # --- 6. UPSERT TO DATABASE ---
                supabase.table("signal_watchlist").upsert({
                    "symbol": symbol,
                    "direction": final_dir,
                    "rsi_touch_value": float(curr_rsi),
                    "extreme_price": float(ext_price),
                    "atr": float(atr_val),
                    "is_ready": is_ready,
                    "last_updated": datetime.now().isoformat()
                }, on_conflict="symbol,direction").execute()

        except Exception as e:
            logger.error(f"❌ Error scanning {symbol}: {e}")


if __name__ == "__main__":
    run_sidbot_scanner()