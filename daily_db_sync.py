import os
import time
import psycopg2
from alpaca_trade_api.rest import TimeFrame
from utils import alpaca, get_symbols

# Ensure this is the postgresql:// string, not the https:// url
DB_CONN_STRING = os.getenv('DATABASE_URL') or os.getenv('SUPABASE_DB_URL')

def update_database():
    all_symbols = get_symbols()  # Uses your existing symbol filter logic
    if not DB_CONN_STRING:
        print("Error: Database connection string not found in environment variables.")
        return

    try:
        conn = psycopg2.connect(DB_CONN_STRING)
        cur = conn.cursor()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    timeframes = {
        '1Day': TimeFrame.Day,
        '4Hour': TimeFrame.Hour * 4,
        '1Hour': TimeFrame.Hour,
        '15Min': TimeFrame.Minute * 15
    }

    # Batching to 200 symbols per request (Alpaca's sweet spot)
    batch_size = 200
    for i in range(0, len(all_symbols), batch_size):
        batch = all_symbols[i:i + batch_size]

        for tf_name, alpaca_tf in timeframes.items():
            print(f"Syncing {tf_name} for batch starting with {batch[0]}...")

            try:
                # Multi-symbol request: 1 call replaces 200 individual calls
                bars_df = alpaca.get_bars(batch, alpaca_tf, limit=1).df

                if not bars_df.empty:
                    for symbol, row in bars_df.iterrows():
                        cur.execute("""
                                    INSERT INTO market_data (symbol, timestamp, timeframe, open, high, low, close, volume)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT ON CONSTRAINT unique_market_bar
                                        DO UPDATE SET open   = EXCLUDED.open,
                                                      high   = EXCLUDED.high,
                                                      low    = EXCLUDED.low,
                                                      close  = EXCLUDED.close,
                                                      volume = EXCLUDED.volume;
                                    """, (symbol.replace('/', ''), row.name, tf_name, float(row['open']), float(row['high']),
                                          float(row['low']), float(row['close']), int(row['volume'])))

                # Manual rate limit to be safe (1 second between timeframe calls)
                time.sleep(1)

            except Exception as e:
                print(f"Error in batch {i} for {tf_name}: {e}")
                continue

        # Commit every batch to keep the WAL log from bloating
        conn.commit()
        print(f"--- Batch complete. {i + len(batch)} symbols processed. ---")

    cur.close()
    conn.close()
    print("Database sync complete.")

if __name__ == "__main__":
    update_database()