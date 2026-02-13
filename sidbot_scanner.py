import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import AverageTrueRange
from populate_db import get_clients

# --- 0. LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_weekly_rsi_resampled(df_daily):
    temp_df = df_daily.copy()
    temp_df["timestamp"] = pd.to_datetime(temp_df["timestamp"])
    temp_df.set_index("timestamp", inplace=True)
    df_weekly = temp_df.resample("W-FRI").agg({
        "open": "first", "high": "max", "low": "min", "close": "last"
    }).dropna()
    if len(df_weekly) < 15: return None, None
    rsi_ser = RSIIndicator(close=df_weekly["close"], window=14).rsi()
    return rsi_ser.iloc[-1], rsi_ser.iloc[-2]


def detect_reversal_pattern(df, direction):
    """Detects simple reversal patterns (Hammer for Long, Shooting Star for Short)."""
    last_row = df.iloc[-1]
    body = abs(last_row['close'] - last_row['open'])
    wick_high = last_row['high'] - max(last_row['open'], last_row['close'])
    wick_low = min(last_row['open'], last_row['close']) - last_row['low']
    if direction == 'LONG':
        return 1 if (wick_low > body * 2 and wick_high < body) else 0
    else:
        return 1 if (wick_high > body * 2 and wick_low < body) else 0


def detect_macd_crossover(df, direction):
    """Detects if MACD Line has crossed the Signal Line."""
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    macd_line = exp1 - exp2
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    curr_m, prev_m = macd_line.iloc[-1], macd_line.iloc[-2]
    curr_s, prev_s = signal_line.iloc[-1], signal_line.iloc[-2]
    if direction == 'LONG':
        return 1 if (prev_m < prev_s and curr_m > curr_s) else 0
    else:
        return 1 if (prev_m > prev_s and curr_m < curr_s) else 0


def run_sidbot_scanner():
    clients = get_clients()
    supabase = clients["supabase_client"]
    tickers_resp = supabase.table("ticker_metadata").select("symbol").execute()
    symbols = [item["symbol"] for item in tickers_resp.data]

    # FETCH MARKET CONTEXT (SPY Alignment)
    spy_data = supabase.table("market_data").select("close").eq("symbol", "SPY").eq("timeframe", "1Day").order(
        "timestamp", desc=True).limit(2).execute()
    spy_up = spy_data.data[0]['close'] > spy_data.data[1]['close'] if len(spy_data.data) > 1 else True

    for symbol in symbols:
        try:
            data_resp = supabase.table("market_data").select("*").eq("symbol", symbol).eq("timeframe", "1Day").order(
                "timestamp", desc=True).limit(100).execute()
            if len(data_resp.data) < 35: continue
            df = pd.DataFrame(data_resp.data).iloc[::-1]

            # Indicators
            rsi_daily = RSIIndicator(close=df["close"]).rsi()
            macd_obj = MACD(close=df["close"])
            macd_line = macd_obj.macd()
            atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"]).average_true_range().iloc[-1]

            curr_rsi, prev_rsi = rsi_daily.iloc[-1], rsi_daily.iloc[-2]
            curr_macd, prev_macd = macd_line.iloc[-1], macd_line.iloc[-2]
            curr_w_rsi, prev_w_rsi = get_weekly_rsi_resampled(df)
            if curr_w_rsi is None: continue

            # Direction (Momentum Room 45/55)
            final_dir = None
            if curr_rsi <= 45:
                final_dir = "LONG"
            elif curr_rsi >= 55:
                final_dir = "SHORT"

            existing = supabase.table("signal_watchlist").select("*").eq("symbol", symbol).execute()
            if existing.data and not final_dir:
                final_dir = existing.data[0]["direction"]

            if final_dir:
                # GATES (Hard Rules)
                d_rsi_ok = (curr_rsi > prev_rsi) if final_dir == "LONG" else (curr_rsi < prev_rsi)
                w_rsi_ok = (curr_w_rsi > prev_w_rsi) if final_dir == "LONG" else (curr_w_rsi < prev_w_rsi)
                macd_ok = (curr_macd > prev_macd) if final_dir == "LONG" else (curr_macd < prev_macd)
                is_ready = all([d_rsi_ok, w_rsi_ok, macd_ok])

                # SCORING (Conviction Points)
                pattern_score = detect_reversal_pattern(df, final_dir)
                alignment_score = 1 if (spy_up if final_dir == "LONG" else not spy_up) else 0
                cross_score = detect_macd_crossover(df, final_dir)
                total_score = int(pattern_score + alignment_score + cross_score)

                logic_trail = {
                    "d_rsi": round(float(curr_rsi), 1),
                    "w_rsi": round(float(curr_w_rsi), 1),
                    "macd_ready": bool(macd_ok),
                    "score": total_score  # This is now the 0-3 Conviction Score
                }

                # Dynamic Stop Loss
                low_val, high_val = df["low"].iloc[-1], df["high"].iloc[-1]
                if existing.data:
                    old_ext = existing.data[0].get("extreme_price")
                    ext_price = min(low_val, old_ext) if final_dir == "LONG" else max(high_val, old_ext)
                else:
                    ext_price = low_val if final_dir == "LONG" else high_val

                # Upsert to DB
                supabase.table("signal_watchlist").upsert({
                    "symbol": symbol, "direction": final_dir, "is_ready": bool(is_ready),
                    "extreme_price": float(ext_price), "rsi_touch_value": float(curr_rsi),
                    "atr": float(atr), "logic_trail": logic_trail, "last_updated": datetime.now().isoformat()
                }, on_conflict="symbol").execute()

        except Exception as e:
            logger.error(f"❌ Error scanning {symbol}: {e}")


if __name__ == "__main__":
    run_sidbot_scanner()