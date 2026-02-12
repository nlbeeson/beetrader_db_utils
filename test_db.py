import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
url = os.getenv('SUPABASE_DB_URL')

try:
    print(f"Connecting to: {url.split('@')[1]}")
    # We add connect_timeout and sslmode here as backups
    conn = psycopg2.connect(url, connect_timeout=10)
    print("Success! The hangar doors are open.")
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")