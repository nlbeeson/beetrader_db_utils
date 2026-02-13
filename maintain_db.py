import os
import logging
from dotenv import load_dotenv
from supabase import create_client

# --- 0. LOGGING SETUP ---
logger = logging.getLogger(__name__)

def purge_rotating_data():
    # Load configuration
    load_dotenv()
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY')
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("❌ Supabase credentials not found in .env")
        return

    # Initialize Client
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Retention Policy:
    # 15Min: 180 Days
    # 1Hour: 1 Year
    # 4Hour: 2 Years
    # Daily: Forever
    
    # Note: VACUUM usually cannot be run inside a transaction or via standard RPC 
    # unless 'run_sql' is specifically configured to handle it (which is rare/risky).
    # It's better to manage VACUUM via Supabase's built-in maintenance or a direct DB connection.
    
    queries = [
        "DELETE FROM market_data WHERE timeframe = '15Min' AND timestamp < NOW() - INTERVAL '180 days';",
        "DELETE FROM market_data WHERE timeframe = '1Hour' AND timestamp < NOW() - INTERVAL '1 year';",
        "DELETE FROM market_data WHERE timeframe = '4Hour' AND timestamp < NOW() - INTERVAL '2 years';"
    ]

    for q in queries:
        try:
            # Assumes a stored procedure 'run_sql_maintenance' exists that executes the string
            # Warning: Allowing raw SQL execution via RPC is a security risk. 
            # Prefer specific RPCs for specific tasks if possible.
            supabase.rpc("run_sql_maintenance", {"query": q}).execute()
            logger.info(f"✅ Executed: {q[:40]}...")
        except Exception as e:
            logger.warning(f"⚠️ Task failed: {e}")


if __name__ == "__main__":
    purge_rotating_data()
