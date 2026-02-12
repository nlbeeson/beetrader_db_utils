# BeeTrader Database Utilities

A collection of maintenance scripts for the BeeTrader database.

## Scripts

- `daily_db_sync.py`: Syncs the latest market data (last 1 bar) for all symbols across all timeframes. Uses `psycopg2` for direct database connection.
- `backfill_db.py`: Backfills historical market data for all symbols. Uses the Supabase HTTP API (`supabase-py`).
- `db_maintenance.py`: Runs `VACUUM ANALYZE` on the `market_data` table to optimize performance.

## Setup

1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure environment variables in a `.env` file (see `.env.example` for required fields).

## Environment Variables

- `APCA_API_KEY_ID`: Alpaca API Key.
- `APCA_API_SECRET_KEY`: Alpaca Secret Key.
- `APCA_URL`: Alpaca API URL (default: `https://paper-api.alpaca.markets`).
- `SUPABASE_URL`: Supabase project URL (used by `backfill_db.py`).
- `SUPABASE_SERVICE_KEY`: Supabase service role key (used by `backfill_db.py`).
- `SUPABASE_DB_URL`: PostgreSQL connection string (e.g., `postgresql://postgres:password@db.xxxx.supabase.co:5432/postgres`).
- `DATABASE_URL`: Alternative to `SUPABASE_DB_URL`.
