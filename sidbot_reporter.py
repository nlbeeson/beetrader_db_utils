import os
import resend
import json
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
            query = supabase.table("signal_watchlist").select("*").execute()
            data = query.data
        except Exception:
            data = []

    # Even if empty, we continue to generate the heartbeat for the email
    confirmed_rows = ""
    potential_rows = ""
    ready_count = 0
    pattern_count = 0

    if data:
        for row in data:
            symbol = row['symbol']
            direction = row['direction']
            trail = row.get('logic_trail', {})
            conf = trail.get('confirmations', {})

            # Align with sidbot_scanner logic_trail: {"d_rsi": ..., "w_rsi": ..., "macd_ready": ...}
            d_rsi = trail.get('d_rsi', 0)
            w_rsi = trail.get('w_rsi', 0)
            macd_ready = trail.get('macd_ready', False)

            # Update stats
            if row['is_ready']: ready_count += 1
            
            # Confidence score calculation (since it's not in DB)
            # Max 3: RSI touch (implied by being in watchlist), Weekly RSI alignment, MACD alignment
            score = 1 # 1 for being here (RSI touch)
            if macd_ready: score += 1
            if row['is_ready']:
                score = 3
            elif macd_ready:
                score = 2

            # HTML Table Row Construction
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
                </tr>
            """

            if row['is_ready']:
                confirmed_rows += html_row
            else:
                potential_rows += html_row

    # 2. Assemble Full HTML Document
    html_content = f"""
    <html>
    <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6;">
        <div style="max-width: 800px; margin: auto; padding: 20px; border: 1px solid #eee; border-radius: 10px;">
            <h2 style="text-align: center; color: #2c3e50;">SidBot Daily Report</h2>
            <p style="text-align: center; color: #7f8c8d;">{datetime.now().strftime('%A, %b %d, %Y')}</p>

            <h3 style="color: #e74c3c; border-bottom: 2px solid #e74c3c; padding-bottom: 5px;">🔥 CONFIRMED ENTRIES (Hard Rules Met)</h3>
            <table style="width: 100%; border-collapse: collapse; margin-bottom: 30px;">
                <thead style="background: #f8f9fa;">
                    <tr><th>Symbol</th><th>Dir</th><th>Stop</th><th>Score</th><th>RSI (D/W)</th><th>MACD</th></tr>
                </thead>
                <tbody>{confirmed_rows if confirmed_rows else '<tr><td colspan="6" style="text-align:center; padding:20px;">No momentum matches in current cycle.</td></tr>'}</tbody>
            </table>

            <h3 style="color: #3498db; border-bottom: 2px solid #3498db; padding-bottom: 5px;">⏳ WATCHLIST (Waiting Room)</h3>
            <table style="width: 100%; border-collapse: collapse; margin-bottom: 30px;">
                <thead style="background: #f8f9fa;">
                    <tr><th>Symbol</th><th>Dir</th><th>Stop</th><th>Score</th><th>RSI (D/W)</th><th>MACD</th></tr>
                </thead>
                <tbody>{potential_rows if potential_rows else '<tr><td colspan="6" style="text-align:center; padding:20px;">No tickers currently at RSI extremes.</td></tr>'}</tbody>
            </table>

            <div style="margin-top: 30px; padding: 15px; background: #f1f5f9; border-radius: 8px; border-left: 5px solid #3182ce;">
                <strong style="color: #2c5282;">Scanner Heartbeat:</strong><br>
                <span style="font-size: 14px; color: #2d3748;">
                    Watchlist Size: {len(data) if data else 0} | 
                    Ready for Entry: {ready_count} | 
                    Chart Patterns Found: {pattern_count}
                </span>
            </div>

            <div style="margin-top: 40px; padding: 20px; background-color: #fff5f5; border: 1px solid #feb2b2; border-radius: 8px;">
                <p style="font-size: 13px; color: #c53030; margin: 0; font-weight: bold;">⚠️ Risk Disclosure & Disclaimer</p>
                <p style="font-size: 12px; color: #742a2a; margin-top: 8px; line-height: 1.4;">
                    This report is for <strong>educational and experimental purposes only</strong>. The signals provided are generated by an automated quantitative system in a testing phase and do not constitute financial advice. Trading involves significant risk of loss. Always perform manual due diligence and consult with a licensed professional before executing any trades.
                </p>
                <p style="font-size: 11px; color: #9b2c2c; margin-top: 10px; font-style: italic; text-align: center;">
                    BeeTrader SidBot v1.0 | Mint Desktop | System Time: {datetime.now().strftime('%H:%M:%S')}
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    return ready_count, pattern_count, html_content


def send_report():
    ready, patterns, html_body = generate_html_report()

    try:
        resend.Emails.send({
            "from": "SidBot Advisor <advisor@notifications.natebeeson.com>",
            "to": [EMAIL_RECEIVER],
            "subject": f"SidBot: {ready} Ready | {patterns} Patterns Found",
            "html": html_body
        })
        print("✅ HTML Intelligence Report sent successfully.")
    except Exception as e:
        print(f"❌ Error sending report: {e}")


if __name__ == "__main__":
    send_report()