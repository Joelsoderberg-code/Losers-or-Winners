import os
from polygon import RESTClient
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Ladda miljövariabler
load_dotenv()
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

print("Skript startar...")
print(f"API-nyckel hittad: {'Ja' if POLYGON_API_KEY else 'Nej'}")

#initiera Polygon client

client = RESTClient(POLYGON_API_KEY)

#initiera Postgres connection

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", 5432)),
    database=os.getenv("POSTGRES_DB", "stocks"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "password")
)


cur = conn.cursor()

#skapa tabell om den inte finns
cur.execute("""
CREATE TABLE IF NOT EXISTS stock_data (
    ticker TEXT,
    date DATE,
    open NUMERIC,
    close NUMERIC,
    volume BIGINT,
    PRIMARY KEY (ticker, date)
)
""")
conn.commit()


# Exempel: hämta dagliga aggregerade priser för AAPL
ticker = "AAPL"
start_date = "2025-08-01"
end_date = "2025-08-10"

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

for bar in aggs:
    cur.execute("""
        INSERT INTO stock_data (ticker, date, open, close, volume)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (ticker, date) DO NOTHING
    """, (
        ticker,
        datetime.fromtimestamp(bar.timestamp / 1000).date(),  # Ändra från bar.t till bar.timestamp
        bar.open,   # Ändra från bar.o till bar.open
        bar.close,  # Ändra från bar.c till bar.close
        bar.volume  # Ändra från bar.v till bar.volume
    ))

conn.commit()
print(f"Sparade {len(aggs)} rader i Postgres för {ticker}")