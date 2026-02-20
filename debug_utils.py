import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD
from populate_db import get_clients


def debug_symbol(symbol):
    clients = get_clients()
    supabase = clients['supabase_client']

    # Fetch data
    data = supabase.table("market_data").select("*").eq("symbol", symbol).eq("timeframe", "1d").order("timestamp",
                                                                                                        desc=True).limit(
        100).execute()
    if not data.data:
        print(f"No data found for {symbol}")
        return

    df = pd.DataFrame(data.data).iloc[::-1]
    rsi_ser = RSIIndicator(close=df['close']).rsi()
    macd_ser = MACD(close=df['close']).macd()

    curr_rsi, prev_rsi = rsi_ser.iloc[-1], rsi_ser.iloc[-2]
    curr_macd, prev_macd = macd_ser.iloc[-1], macd_ser.iloc[-2]

    print(f"--- DEBUG: {symbol} ---")
    print(f"Daily RSI: {curr_rsi:.2f} (Prev: {prev_rsi:.2f})")
    print(f"MACD Line: {curr_macd:.4f} (Prev: {prev_macd:.4f})")

    # Check Weekly
    temp_df = df.copy()
    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
    temp_df.set_index('timestamp', inplace=True)
    df_w = temp_df.resample('W-FRI').agg({'close': 'last'}).dropna()
    if len(df_w) >= 14:
        rsi_w = RSIIndicator(close=df_w['close']).rsi()
        print(f"Weekly RSI: {rsi_w.iloc[-1]:.2f} (Prev: {rsi_w.iloc[-2]:.2f})")
    else:
        print("Not enough weekly data for RSI")


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        debug_symbol(sys.argv[1])