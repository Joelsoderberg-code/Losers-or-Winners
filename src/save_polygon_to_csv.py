import os
from polygon import RESTClient
import csv
from datetime import datetime
from dotenv import load_dotenv

# Ladda miljövariabler
load_dotenv()
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

print("Skript startar...")
print(f"API-nyckel hittad: {'Ja' if POLYGON_API_KEY else 'Nej'}")

# initiera Polygon client
client = RESTClient(POLYGON_API_KEY)

# Exempel: hämta dagliga aggregerade priser för AAPL
ticker = "AAPL"
start_date = "2025-09-01"
end_date = "2025-09-03"

aggs = client.get_aggs(
    ticker,
    1,               # 1 betyder "1-enhet" (day = daglig)
    "day",           # tidsenhet: day
    start_date,
    end_date
)

print(f"Hämtade {len(aggs)} datapunkter för {ticker}")

# Lägg till debug för att se strukturen
if len(aggs) > 0:
    first_bar = aggs[0]
    print(f"Första datapunkten: {first_bar}")
    print(f"Typ: {type(first_bar)}")
    print(f"Dir: {dir(first_bar)}")  # Visar alla tillgängliga attribut/metoder
    print(f"Dict: {vars(first_bar)}")  # Visar alla attribut som dictionary

# Förbered rader för CSV
rows = []
for bar in aggs:
    rows.append({
        "ticker": ticker,
        "date": datetime.fromtimestamp(bar.timestamp / 1000).date().isoformat(),
        "open": bar.open,
        "close": bar.close,
        "volume": bar.volume,
    })

# Skriv till CSV
output_dir = "/home/joel/Losers-or-Winners/data"
os.makedirs(output_dir, exist_ok=True)
csv_path = os.path.join(output_dir, "stock_data.csv")

with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["ticker", "date", "open", "close", "volume"])
    writer.writeheader()
    writer.writerows(rows)

print(f"Skrev {len(rows)} rader till {csv_path}")


