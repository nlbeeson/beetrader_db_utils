import supabase

def purge_old_data():
    # Define your retention policies
    queries = [
        # 1. Purge 15m older than 180 days
        "DELETE FROM market_data WHERE timeframe = '15Min' AND timestamp < NOW() - INTERVAL '180 days';",

        # 2. Purge 1h older than 1 year
        "DELETE FROM market_data WHERE timeframe = '1Hour' AND timestamp < NOW() - INTERVAL '1 year';",

        # 3. Purge 4h older than 2 years (Optional - can be adjusted)
        "DELETE FROM market_data WHERE timeframe = '4Hour' AND timestamp < NOW() - INTERVAL '2 years';",

        # 4. Reclaim physical space (Vacuum)
        "VACUUM market_data;"
    ]

    for q in queries:
        try:
            supabase.rpc("run_sql", {"sql_query": q}).execute()
            print(f"✅ Maintenance Task Complete: {q[:30]}...")
        except Exception as e:
            print(f"⚠️ SQL Task Failed: {e}")


if __name__ == "__main__":
    purge_old_data()