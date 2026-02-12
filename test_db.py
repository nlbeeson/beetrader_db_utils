import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
# Get the RAW URL (without the ?sslmode=... at the end)
url = os.getenv('SUPABASE_DB_URL').split('?')[0]

try:
    print(f"Directing ambulance to: {url.split('@')[1]}")
    # Force SSL and set a longer timeout
    conn = psycopg2.connect(
        url,
        sslmode='require',
        connect_timeout=15
    )
    print("Success! Hangar doors are open.")
    conn.close()
except Exception as e:
    print(f"Handshake failed: {e}")