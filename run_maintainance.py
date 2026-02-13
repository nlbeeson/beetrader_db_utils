import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# You can find your Connection String in Supabase -> Settings -> Database
# It looks like: postgres://postgres.[USER]:[PASSWORD]@[HOST]:5432/postgres
DB_CONNECTION_STRING = os.getenv("SUPABASE_DB_URL")

def run_heavy_commands():
    try:
        # Connect to your postgres DB
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        conn.autocommit = True # Necessary for VACUUM and CLUSTER
        cur = conn.cursor()

        print("🚀 Starting ANALYZE... (this may take a few minutes)")
        cur.execute("ANALYZE public.market_data;")
        print("✅ ANALYZE Complete.")

        print("🚀 Starting REINDEX... (this may take a few minutes)")
        cur.execute("REINDEX INDEX idx_market_data_lookup;")
        print("✅ REINDEX Complete.")

        print("🚀 Starting CLUSTER... (Attempting to bypass timeout)")
        # This sets the timeout to 2 hours
        cur.execute("SET statement_timeout = '2h';")
        cur.execute("CLUSTER public.market_data USING idx_market_data_lookup;")
        print("✅ CLUSTER Complete.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_heavy_commands()