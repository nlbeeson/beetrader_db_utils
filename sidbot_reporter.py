import os
import resend
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

# --- CONFIG ---
load_dotenv()
resend.api_key = os.getenv("RESEND_API_KEY")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


def get_tv_url(symbol):
    """Generates a TradingView chart URL for the symbol."""
    return f"https://www.tradingview.com/chart/?symbol={symbol}"


def get_supabase():
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            return None
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        return None


def generate_html_report():
    supabase = get_supabase()

    # 1. Fetch all signals current in the watchlist
    data = []
    if supabase:
        try:
            # Explicitly select next_earnings column
            query = supabase.table("signal_watchlist").select("*").execute()
            data = query.data
        except Exception:
            data = []

    confirmed_rows = ""
    potential_rows = ""
    ready_count = 0
    pattern_count = 0

    if data:
        for row in data:
            symbol = row['symbol']
            direction = row['direction']
            trail = row.get('logic_trail', {})

            # Fetch indicators from trail
            d_rsi = trail.get('d_rsi', 0)
            w_rsi = trail.get('w_rsi', 0)
            macd_ready = trail.get('macd_ready', False)

            # --- NEW: Earnings Date Processing ---
            earnings_val = row.get('next_earnings', 'N/A')
            earnings_display = earnings_val

            if earnings_val and earnings_val != 'N/A':
                try:
                    # Format for better readability (e.g., Feb 13)
                    earn_dt = datetime.strptime(earnings_val, '%Y-%m-%d')
                    earnings_display = earn_dt.strftime('%b %d')

                    # Optional: Add a warning color if earnings are very close
                    days_to = (earn_dt.date() - datetime.now().date()).days
                    if 0 <= days_to <= 3:
                        earnings_display = f'<span style="color: #e74c3c; font-weight: bold;">⚠️ {earnings_display}</span>'
                except Exception:
                    pass

            # Update stats
            if row['is_ready']: ready_count += 1

            # Confidence score calculation
            score = 1
            if macd_ready: score += 1
            if row['is_ready']:
                score = 3
            elif macd_ready:
                score = 2

            # HTML Table Row Construction (Added Earnings Column)
            html_row = f"""
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">
                        <a href="{get_tv_url(symbol)}" style="color: #2962ff; font-weight: bold; text-decoration: none;">{symbol}</a>
                    </td>
                    <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">{direction}</td>
                    <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">${float(row.get('extreme_price', 0) or 0):.2f}</td>
                    <td style="padding: 10px; border: 1px solid #ddd; text-align: center; font-weight: bold;">{score}/3</td>
                    <td style="padding: 10px; border: 1px solid #ddd; text-align: center; color: #555;">D:{d_rsi:.1f} W:{w_rsi:.1f}</td>
                    <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">{'✅' if macd_ready else '❌'}</td>
                    <td style="padding: 10px; border: 1px solid #ddd; text-align: center; font-size: 13px;">{earnings_display}</td>
                </tr>
            """

            if row['is_ready']:
                confirmed_rows += html_row
            else:
                potential_rows += html_row

    # 2. Assemble Full HTML Document (Updated Table Headers)
    html_content = f"""
    <html>
    <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6;">
        <div style="max-width: 900px; margin: auto; padding: 20px; border: 1px solid #eee; border-radius: 10px;">
            <h2 style="text-align: center; color: #2c3e50;">SidBot Daily Report</h2>
            <p style="text-align: center; color: #7f8c8d;">{datetime.now().strftime('%A, %b %d, %Y')}</p>

            <h3 style="color: #e74c3c; border-bottom: 2px solid #e74c3c; padding-bottom: 5px;">🔥 CONFIRMED ENTRIES (Hard Rules Met)</h3>
            <table style="width: 100%; border-collapse: collapse; margin-bottom: 30px;">
                <thead style="background: #f8f9fa;">
                    <tr><th>Symbol</th><th>Dir</th><th>Stop</th><th>Score</th><th>RSI (D/W)</th><th>MACD</th><th>Earnings</th></tr>
                </thead>
                <tbody>{confirmed_rows if confirmed_rows else