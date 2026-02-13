import os
import resend
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
resend.api_key = os.getenv("RESEND_API_KEY")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


def get_tv_url(symbol):
    return f"https://www.tradingview.com/chart/?symbol={symbol}"


def generate_html_report():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    data = supabase.table("signal_watchlist").select("*").execute().data

    confirmed_rows, potential_rows = "", ""
    ready_count = 0

    for row in data:
        symbol = row['symbol']
        direction = row['direction']
        trail = row.get('logic_trail', {})

        # Pull Indicators
        d_rsi, w_rsi = trail.get('d_rsi', 0), trail.get('w_rsi', 0)
        macd_slope = trail.get('macd_ready', False)
        macd_cross = trail.get('macd_cross', False)

        # Earnings Processing
        earnings_val = row.get('next_earnings', 'N/A')
        earnings_display = earnings_val
        if earnings_val and earnings_val != 'N/A':
            try:
                earn_dt = datetime.strptime(earnings_val, '%Y-%m-%d')
                earnings_display = earn_dt.strftime('%b %d')
                days_to = (earn_dt.date() - datetime.now().date()).days
                if 0 <= days_to <= 3:
                    earnings_display = f'<span style="color: #e74c3c; font-weight: bold;">⚠️ {earnings_display}</span>'
            except:
                pass

        if row['is_ready']: ready_count += 1

        # Multiplier Score: Base(1) + Slope(1) + Cross(1)
        score = 1 + (1 if macd_slope else 0) + (1 if macd_cross else 0)
        dir_color = "#27ae60" if direction == "LONG" else "#e74c3c"

        html_row = f"""
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">
                    <a href="{get_tv_url(symbol)}" style="color: #2962ff; font-weight: bold; text-decoration: none;">{symbol}</a>
                </td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">
                    <span style="background: {dir_color}; color: white; padding: 2px 6px; border-radius: 4px; font-size: 11px;">{direction}</span>
                </td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center; font-weight: bold;">{score}/3</td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center; color: #555;">D:{d_rsi:.1f} W:{w_rsi:.1f}</td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">{'✅' if macd_slope else '❌'}</td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">{'✅' if macd_cross else '❌'}</td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center; font-size: 13px;">{earnings_display}</td>
            </tr>
        """
        if row['is_ready']:
            confirmed_rows += html_row
        else:
            potential_rows += html_row

    html_content = f"""
    <html>
    <body style="font-family: sans-serif; color: #333; line-height: 1.6;">
        <div style="max-width: 900px; margin: auto; padding: 20px;">
            <h2 style="text-align: center; color: #2c3e50;">SidBot Daily Intelligence</h2>
            <h3 style="color: #e74c3c; border-bottom: 2px solid #e74c3c;">🔥 CONFIRMED ENTRIES (Ready: {ready_count})</h3>
            <table style="width: 100%; border-collapse: collapse; margin-bottom: 30px;">
                <thead style="background: #f8f9fa;">
                    <tr><th>Symbol</th><th>Dir</th><th>Score</th><th>RSI (D/W)</th><th>Slope</th><th>Cross</th><th>Earnings</th></tr>
                </thead>
                <tbody>{confirmed_rows if confirmed_rows else '<tr><td colspan="7" style="text-align:center; padding:20px;">No confirmed signals.</td></tr>'}</tbody>
            </table>
            <h3 style="color: #3498db; border-bottom: 2px solid #3498db;">⏳ WATCHLIST (Waiting Room)</h3>
            <table style="width: 100%; border-collapse: collapse;">
                <thead style="background: #f8f9fa;">
                    <tr><th>Symbol</th><th>Dir</th><th>Score</th><th>RSI (D/W)</th><th>Slope</th><th>Cross</th><th>Earnings</th></tr>
                </thead>
                <tbody>{potential_rows if potential_rows else '<tr><td colspan="7" style="text-align:center; padding:20px;">No signals found.</td></tr>'}</tbody>
            </table>
        </div>
    </body>
    </html>
    """
    return html_content


def send_report():
    html_body = generate_html_report()
    resend.Emails.send({
        "from": "SidBot Advisor <advisor@notifications.natebeeson.com>",
        "to": [EMAIL_RECEIVER],
        "subject": f"SidBot Daily Report - {datetime.now().strftime('%b %d')}",
        "html": html_body
    })
    print("✅ Report Sent.")


if __name__ == "__main__":
    send_report()