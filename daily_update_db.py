import logging
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from populate_db import get_ticker_universe, populate_lane, get_clients

# --- 0. LOGGING SETUP ---
logger = logging.getLogger(__name__)

def run_daily_update():
    # Initialize clients using the shared helper
    clients = get_clients()
    
    # Use your provided Russell 2000 CSV + Watchlist
    universe = get_ticker_universe()

    # We fetch 5 days of data to cover weekends and holiday gaps
    targets = [
        {"label": "1Day", "tf": TimeFrame.Day, "days": 5},
        {"label": "4Hour", "tf": TimeFrame(4, TimeFrameUnit.Hour), "days": 5},
        {"label": "1Hour", "tf": TimeFrame.Hour, "days": 5},
        {"label": "15Min", "tf": TimeFrame(15, TimeFrameUnit.Minute), "days": 5}
    ]

    for target in targets:
        for a_class, s_list in universe.items():
            # Pass the clients to reuse connections and configuration
            populate_lane(s_list, target["tf"], target["label"], target["days"], a_class, clients=clients)


if __name__ == "__main__":
    run_daily_update()
