import os
import xml.etree.ElementTree as ET
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# 1. Setup Supabase Connection
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_KEY")
supabase = create_client(url, key)


def scrape_russell_xml(file_path):
    # Namespaces found in the XML file
    ns = {
        'ss': 'urn:schemas-microsoft-com:office:spreadsheet',
        'default': 'urn:schemas-microsoft-com:office:spreadsheet'
    }

    tree = ET.parse(file_path)
    root = tree.getroot()

    # 2. Find the "Holdings" Worksheet
    holdings_sheet = None
    for sheet in root.findall('default:Worksheet', ns):
        if sheet.get('{urn:schemas-microsoft-com:office:spreadsheet}Name') == 'Holdings':
            holdings_sheet = sheet
            break

    if not holdings_sheet:
        print("Holdings sheet not found.")
        return

    table = holdings_sheet.find('default:Table', ns)
    rows = table.findall('default:Row', ns)

    # 3. Iterate through rows (Skipping header rows 1-8)
    # Data starts at the row after the headers
    for row in rows[8:]:
        cells = row.findall('default:Cell', ns)
        if len(cells) < 11: continue

        # Extract values based on column position
        ticker = cells[0].find('default:Data', ns).text
        name = cells[1].find('default:Data', ns).text
        sector = cells[2].find('default:Data', ns).text
        asset_class = cells[3].find('default:Data', ns).text
        exchange = cells[10].find('default:Data', ns).text

        # 4. Clean Data and Upsert to Supabase
        # We only want Equities, not cash or derivatives
        if asset_class == 'Equity' and ticker:
            data = {
                "symbol": ticker,
                "company_name": name,
                "sector": sector,
                "exchange": exchange,
                "is_etf": False,
                "is_active": True
            }

            try:
                supabase.table("ticker_reference").upsert(data).execute()
                print(f"Upserted: {ticker}")
            except Exception as e:
                print(f"Error upserting {ticker}: {e}")


# Run the scraper
scrape_russell_xml('iShares-Russell-1000-ETF_fund.xml')