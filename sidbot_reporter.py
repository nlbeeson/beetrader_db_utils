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
    conf_rows, pot_rows, ready_count = "", "", 0

    for row in data:
        symbol, direction = row['symbol'], row['direction']
        trail = row.get('logic_trail', {})
        d_rsi, w_rsi = trail.get('d_rsi', 0), trail.get('w_rsi', 0)
        slope, cross = trail.get('macd_ready', False), trail.get('macd_cross', False)

        # Earnings countdown logic
        earn_val = row.get('next_earnings', 'N/A')
        earn_disp = earn_val
        if earn_val and earn_val != 'N/A':
            try:
                dt = datetime.strptime(earn_val, '%Y-%m-%d').date()
                days = (dt - datetime.now().date()).days
                countdown = f"{days}d" if days >= 0 else "Past"
                earn_disp = f"{dt.strftime('%b %d')} ({countdown})"
                if 0 <= days <= 14:
                    earn_disp = f'<span style="color:#e74c3c;font-weight:bold;">⚠️ {earn_disp}</span>'
            except:
                pass

        if row['is_ready']: ready_count += 1
        score = 1 + (1 if slope else 0) + (1 if cross else 0)
        color = "#27ae60" if direction == "LONG" else "#e74c3c"

        row_html = f"""
            <tr>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;"><a href="{get_tv_url(symbol)}" style="color:#2962ff;font-weight:bold;text-decoration:none;">{symbol}</a></td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;"><span style="background:{color};color:white;padding:2px 6px;border-radius:4px;font-size:11px;">{direction}</span></td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;font-weight:bold;">{score}/3</td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;">D:{d_rsi:.1f} W:{w_rsi:.1f}</td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;">{'✅' if slope else '❌'}</td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;">{'✅' if cross else '❌'}</td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;font-size:12px;">{earn_disp}</td>
            </tr>"""
        if row['is_ready']:
            conf_rows += row_html
        else:
            pot_rows += row_html

    return f"""<html><body style="font-family:sans-serif;color:#333;line-height:1.6;"><div style="max-width:950px;margin:auto;padding:20px;">
        <h2 style="text-align:center;color:#2c3e50;">SidBot Daily Intelligence</h2>
        <h3 style="color:#e74c3c;border-bottom:2px solid #e74c3c;">🔥 CONFIRMED ENTRIES (Ready: {ready_count})</h3>
        <table style="width:100%;border-collapse:collapse;margin-bottom:30px;">
            <thead style="background:#f8f9fa;"><tr><th>Symbol</th><th>Dir</th><th>Score</th><th>RSI (D/W)</th><th>Slope</th><th>Cross</th><th>Earnings (Days)</th></tr></thead>
            <tbody>{conf_rows if conf_rows else '<tr><td colspan="7" style="text-align:center;">No confirmed signals.</td></tr>'}</tbody>
        </table>
        <h3 style="color:#3498db;border-bottom:2px solid #3498db;">⏳ WATCHLIST (Waiting Room)</h3>
        <table style="width:100%;border-collapse:collapse;">
            <thead style="background:#f8f9fa;"><tr><th>Symbol</th><th>Dir</th><th>Score</th><th>RSI (D/W)</th><th>Slope</th><th>Cross</th><th>Earnings (Days)</th></tr></thead>
            <tbody>{pot_rows if pot_rows else '<tr><td colspan="7" style="text-align:center;">No signals found.</td></tr>'}</tbody>
        </table></div></body></html>"""


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