import os
import resend
from datetime import datetime
from dotenv import load_dotenv
from populate_db import get_clients

# --- CONFIG ---
load_dotenv()
resend.api_key = os.getenv("RESEND_API_KEY")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")


def get_tv_url(symbol):
    """Generates a TradingView chart URL."""
    return f"https://www.tradingview.com/chart/?symbol={symbol}"


def generate_html_report():
    clients = get_clients()
    supabase = clients['supabase_client']

    query = supabase.table("signal_watchlist").select("*").execute()
    data = query.data

    if not data:
        return 0, 0, 0, "<h2>No active signals found in database.</h2>"

    confirmed_rows = ""
    potential_rows = ""
    pattern_count = 0
    ready_count = 0

    for row in data:
        symbol = row['symbol']
        direction = row['direction']
        trail = row.get('logic_trail', {})
        conf = trail.get('confirmations', {})

        # Track Stats
        if row['is_ready']: ready_count += 1
        if conf.get('pattern'): pattern_count += 1

        # Pattern Labeling
        pattern_label = "None"
        if conf.get('pattern'):
            pattern_label = "Double Bottom" if direction == "LONG" else "Double Top"
            pattern_label += f" (Δ{conf.get('pattern_spread', 0):.2f})"

        mkt_status = '✅' if conf.get('market') else '❌'
        sec_status = '✅' if conf.get('sector') else '❌'

        html_row = f"""
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">
                    <a href="{get_tv_url(symbol)}" style="color: #2962ff; font-weight: bold; text-decoration: none;">{symbol}</a>
                </td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">{direction}</td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">${row['extreme_price']:.2f}</td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">{row['confidence_score']}/3</td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">{pattern_label}</td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">M:{mkt_status} S:{sec_status}</td>
            </tr>
        """
        if row['is_ready']:
            confirmed_rows += html_row
        else:
            potential_rows += html_row

    # Assemble HTML
    html_content = f"""
    <html>
    <body style="font-family: sans-serif; color: #333;">
        <div style="max-width: 800px; margin: auto; border: 1px solid #eee; padding: 20px;">
            <h2 style="text-align: center;">SidBot Intelligence Report</h2>

            <h3 style="color: #e74c3c;">🔥 CONFIRMED ENTRIES (Momentum Ready)</h3>
            <table style="width: 100%; border-collapse: collapse;">
                <thead style="background: #f8f9fa;">
                    <tr><th>Symbol</th><th>Dir</th><th>Stop</th><th>Score</th><th>Pattern</th><th>Context</th></tr>
                </thead>
                <tbody>{confirmed_rows if confirmed_rows else '<tr><td colspan="6" style="text-align:center;">No momentum matches.</td></tr>'}</tbody>
            </table>

            <div style="margin-top: 30px; padding: 15px; background: #f1f5f9; border-radius: 5px;">
                <strong>Scanner Heartbeat:</strong> Scanned {len(data)} Watchlist Tickers | {ready_count} Momentum Ready | {pattern_count} Patterns Detected
            </div>

            <div style="margin-top: 40px; padding: 15px; background: #fff5f5; border: 1px solid #feb2b2; font-size: 12px; color: #742a2a;">
                <strong>⚠️ Disclaimer:</strong> Educational use only. Generated from an experimental quantitative system. Trading stocks involves significant risk of loss. Always perform manual due diligence before entry.
            </div>
        </div>
    </body>
    </html>
    """
    return len(data), ready_count, pattern_count, html_content


def send_report():
    total, ready, patterns, html_body = generate_html_report()
    try:
        resend.Emails.send({
            "from": "SidBot <onboarding@resend.dev>",
            "to": [EMAIL_RECEIVER],
            "subject": f"SidBot: {ready} Ready | {patterns} Patterns Found",
            "html": html_body
        })
        print("✅ Report sent.")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    send_report()