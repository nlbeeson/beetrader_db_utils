import os
import logging
import pandas as pd
import json
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

SECTOR_MAP = {
    'Information Technology': 'XLK', 'Financials': 'XLF', 'Health Care': 'XLV',
    'Energy': 'XLE', 'Utilities': 'XLU', 'Consumer Discretionary': 'XLY',
    'Consumer Staples': 'XLP', 'Industrials': 'XLI', 'Materials': 'XLB',
    'Real Estate': 'XLRE', 'Communication Services': 'XLC', 'Communication': 'XLC'
}


def get_weekly_rsi_resampled(df_daily):
    temp_df = df_daily.copy()
    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
    temp_df.set_index('timestamp', inplace=True)
    df_weekly = temp_df.resample('W-FRI').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
    }).dropna()
    if len(df_weekly) < 15: return None, None
    rsi_weekly = RSIIndicator(close=df_weekly['close'], window=14).rsi()
    return rsi_weekly.iloc[-1], rsi_weekly.iloc[-2]


def check_etf_alignment(etf_symbol, direction, supabase):
    try:
        data = supabase.table("market_data").select("close").eq("symbol", etf_symbol).eq("timeframe", "1Day").order(
            "timestamp", desc=True).limit(30).execute()
        if len(data.data) < 26: return False
        df = pd.DataFrame(data.data).iloc[::-1]
        rsi = RSIIndicator(close=df['close']).rsi()
        macd_line = MACD(close=df['close']).macd()
        rsi_up = rsi.iloc[-1] > rsi.iloc[-2]
        macd_up = macd_line.iloc[-1] > macd_line.iloc[-2]
        return (rsi_up and macd_up) if direction == 'LONG' else ((not rsi_up) and (not macd_up))
    except Exception:
        return False


def check_reversal_pattern(symbol, current_low, current_high, extreme_price, direction, atr):
    if not extreme_price: return False, 0
    buffer = 0.5 * atr
    diff = abs(current_low - extreme_price) if direction == 'LONG' else abs(current_high - extreme_price)
    is_pattern = diff <= buffer
    return is_pattern, float(diff)


def run_sidbot_scanner():
    clients = get_clients()
    supabase = clients['supabase_client']
    logger.info("🧹 Pruning signals older than 21 days...")
    cutoff = (datetime.now() - timedelta(days=21)).isoformat()
    supabase.table("signal_watchlist").delete().lt("rsi_touch_date", cutoff).execute()

    active_tickers_resp = supabase.table("market_data").select("symbol").eq("timeframe", "1Day").execute()
    symbols = list(set([item['symbol'] for item in active_tickers_resp.data]))

    for symbol in symbols:
        try:
            daily_data = supabase.table("market_data").select("*").eq("symbol", symbol).eq("timeframe", "1Day").order(
                "timestamp", desc=True).limit(100).execute()
            if len(daily_data.data) < 26: continue
            df_daily = pd.DataFrame(daily_data.data).iloc[::-1]

            rsi_daily_ser = RSIIndicator(close=df_daily['close']).rsi()
            macd_line = MACD(close=df_daily['close']).macd()
            atr_val = AverageTrueRange(high=df_daily['high'], low=df_daily['low'],
                                       close=df_daily['close']).average_true_range().iloc[-1]
            curr_rsi, prev_rsi = rsi_daily_ser.iloc[-1], rsi_daily_ser.iloc[-2]
            curr_macd, prev_macd = macd_line.iloc[-1], macd_line.iloc[-2]
            curr_w_rsi, prev_w_rsi = get_weekly_rsi_resampled(df_daily)
            if curr_w_rsi is None: continue

            direction = 'LONG' if curr_rsi <= 30 else ('SHORT' if curr_rsi >= 70 else None)
            existing = supabase.table("signal_watchlist").select("*").eq("symbol", symbol).execute()

            if direction or existing.data:
                final_dir = direction if direction else existing.data[0]['direction']
                ext_price = existing.data[0]['extreme_price'] if existing.data else df_daily['close'].iloc[-1]

                # Rule Logic
                daily_rsi_up = curr_rsi > prev_rsi if final_dir == 'LONG' else curr_rsi < prev_rsi
                weekly_rsi_up = curr_w_rsi > prev_w_rsi if final_dir == 'LONG' else curr_w_rsi < prev_w_rsi
                macd_up = curr_macd > prev_macd if final_dir == 'LONG' else curr_macd < prev_macd
                is_ready = all([daily_rsi_up, weekly_rsi_up, macd_up])

                # Confirmations
                mkt_aligned = check_etf_alignment('SPY', final_dir, supabase)
                meta = supabase.table("ticker_metadata").select("sector").eq("symbol", symbol).execute()
                sec_ticker = SECTOR_MAP.get(meta.data[0]['sector']) if meta.data else None
                sec_aligned = check_etf_alignment(sec_ticker, final_dir, supabase) if sec_ticker else False
                pattern_found, spread = check_reversal_pattern(symbol, df_daily['low'].iloc[-1],
                                                               df_daily['high'].iloc[-1], ext_price, final_dir, atr_val)

                score = sum([mkt_aligned, sec_aligned, pattern_found])
                trail = {
                    "rules": {"d_rsi": daily_rsi_up, "w_rsi": weekly_rsi_up, "macd": macd_up},
                    "confirmations": {"market": mkt_aligned, "sector": sec_aligned, "pattern": pattern_found,
                                      "pattern_spread": spread},
                    "score": score
                }

                if is_ready:
                    logger.info(f"🔥 READY: {symbol} Score:{score} Trail:{json.dumps(trail)}")

                supabase.table("signal_watchlist").upsert({
                    "symbol": symbol, "direction": final_dir, "is_ready": is_ready, "confidence_score": score,
                    "logic_trail": trail, "extreme_price": float(ext_price), "atr": float(atr_val),
                    "last_updated": datetime.now().isoformat()
                }, on_conflict="symbol,direction").execute()
        except Exception as e:
            logger.error(f"Error {symbol}: {e}")


if __name__ == "__main__": run_sidbot_scanner()