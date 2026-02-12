import os
import psycopg2
from psycopg2 import extensions
from dotenv import load_dotenv

load_dotenv()
DB_CONN_STRING = os.getenv('SUPABASE_DB_URL') or os.getenv('DATABASE_URL')


def run_maintenance():
    # Vacuum cannot run inside a transaction block
    conn = psycopg2.connect(DB_CONN_STRING)
    conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    print("Initiating Saturday Night Deep Clean...")
    try:
        cur.execute("VACUUM ANALYZE market_data;")
        print("Vacuum and Analyze complete. The hangar is clean.")
    except Exception as e:
        print(f"Maintenance failed: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_maintenance()