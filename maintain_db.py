import supabase

def purge_rotating_data():
    # Retention Policy:
    # 15Min: 180 Days (Execution context)
    # 1Hour: 1 Year (Intraday trend)
    # 4Hour: 2 Years (Swing trend - Keep this for your prop firm strategies)
    # Daily: Forever (Backtesting & Portfolio tracking)

    queries = [
        "DELETE FROM market_data WHERE timeframe = '15Min' AND timestamp < NOW() - INTERVAL '180 days';",
        "DELETE FROM market_data WHERE timeframe = '1Hour' AND timestamp < NOW() - INTERVAL '1 year';",
        "DELETE FROM market_data WHERE timeframe = '4Hour' AND timestamp < NOW() - INTERVAL '2 years';",
        "VACUUM market_data;"  # Reclaims physical disk space
    ]

    for q in queries:
        try:
            # You'll need to enable a 'run_sql' RPC in Supabase or use a direct Postgres connection
            supabase.postgrest.rpc("run_sql_maintenance", {"query": q}).execute()
            print(f"✅ Executed: {q[:40]}...")
        except Exception as e:
            print(f"⚠️ Purge failed: {e}")


if __name__ == "__main__":
    purge_rotating_data()