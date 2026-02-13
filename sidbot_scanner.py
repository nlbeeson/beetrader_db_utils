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
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_weekly_rsi_resampled(df_daily):
    """Calculates Weekly RSI by resampling daily data to Friday closes."""
    temp_df = df_daily.copy()
    temp_df["timestamp"] = pd.to_datetime(temp_df["timestamp"])
    temp_df.set_index("timestamp", inplace=True)
    df_weekly = temp_df.resample("W-FRI").agg({
        "open": "first", "high": "max", "low": "min", "close": "last"
    }).dropna()
    if len(df_weekly) < 15: return None, None
    rsi_ser = RSIIndicator(close=df_weekly["close"], window=14).rsi()
    return rsi_ser.iloc[-1], rsi_ser.iloc[-2]


def run_sidbot_scanner():
    clients = get_clients()
    supabase = clients["supabase_client"]

    # 1. Master List from Ticker Metadata
    tickers_resp = supabase.table("ticker_metadata").select("symbol").execute()
    symbols = [item["symbol"] for item in tickers_resp.data]
    logger.info(f"🔎 Scanning {len(symbols)} symbols with 45/55 RSI thresholds...")

    for symbol in symbols:
        try:
            # 2. Get 100 days of data for calculations
            data_resp = supabase.table("market_data").select("*").eq("symbol", symbol).eq("timeframe", "1Day").order(
                "timestamp", desc=True).limit(100).execute()
            if len(data_resp.data) < 35: continue
            df = pd.DataFrame(data_resp.data).iloc[::-1]

            # 3. Indicators
            rsi_daily = RSIIndicator(close=df["close"]).rsi()
            macd_line = MACD(close=df["close"]).macd()
            atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"]).average_true_range().iloc[-1]

            curr_rsi, prev_rsi = rsi_daily.iloc[-1], rsi_daily.iloc[-2]
            curr_macd, prev_macd = macd_line.iloc[-1], macd_line.iloc[-2]
            curr_w_rsi, prev_w_rsi = get_weekly_rsi_resampled(df)
            if curr_w_rsi is None: continue

            # 4. MOMENTUM ROOM LOGIC (Wider Entry)
            final_dir = None
            if curr_rsi <= 45:
                final_dir = "LONG"
            elif curr_rsi >= 55:
                final_dir = "SHORT"

            existing = supabase.table("signal_watchlist").select("*").eq("symbol", symbol).execute()
            if existing.data and not final_dir:
                final_dir = existing.data[0]["direction"]

            if final_dir:
                # 5. THE TURN (Strict Slope Check)
                d_rsi_ok = (curr_rsi > prev_rsi) if final_dir == "LONG" else (curr_rsi < prev_rsi)
                w_rsi_ok = (curr_w_rsi > prev_w_rsi) if final_dir == "LONG" else (curr_w_rsi < prev_w_rsi)
                macd_ok = (curr_macd > prev_macd) if final_dir == "LONG" else (curr_macd < prev_macd)

                is_ready = all([d_rsi_ok, w_rsi_ok, macd_ok])

                # 6. JSON-SAFE LOGIC TRAIL
                logic_trail = {
                    "d_rsi": round(float(curr_rsi), 1),
                    "w_rsi": round(float(curr_w_rsi), 1),
                    "macd_ready": bool(macd_ok),
                    "score": int(sum([d_rsi_ok, w_rsi_ok, macd_ok]))
                }

                # 7. EXTREME PRICE TRACKING
                low_val, high_val = df["low"].iloc[-1], df["high"].iloc[-1]
                if existing.data:
                    old_ext = existing.data[0].get("extreme_price")
                    ext_price = min(low_val, old_ext) if final_dir == "LONG" else max(high_val, old_ext)
                else:
                    ext_price = low_val if final_dir == "LONG" else high_val

                # 8. UPSERT
                supabase.table("signal_watchlist").upsert({
                    "symbol": symbol, "direction": final_dir, "is_ready": bool(is_ready),
                    "extreme_price": float(ext_price), "rsi_touch_value": float(curr_rsi),
                    "atr": float(atr), "logic_trail": logic_trail, "last_updated": datetime.now().isoformat()
                }, on_conflict="symbol").execute()

        except Exception as e:
            logger.error(f"❌ Error scanning {symbol}: {e}")


if __name__ == "__main__":
    run_sidbot_scanner()