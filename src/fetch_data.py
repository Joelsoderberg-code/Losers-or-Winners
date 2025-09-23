# Importerar alla nödvändiga bibliotek:
import csv
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from polygon import RESTClient


def fetch_data_from_api(
    api_key: str | None = None,
    ticker: str | None = None,
    output_path: str | None = None,
) -> None:
    # Läs in .env och miljövariabler om argument inte ges
    load_dotenv()
    api_key = api_key or os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise ValueError("POLYGON_API_KEY saknas i .env/miljön och gavs inte som argument")

    ticker = ticker or os.getenv("TICKER", "SPY")

    if output_path is None:
        output_dir = os.getenv("OUTPUT_DIR", "/home/joel/Losers-or-Winners/data")
        output_file = os.getenv("OUTPUT_FILE", "stock_data.csv")
        output_path = os.path.join(output_dir, output_file)

    # Hämta gårdagens datum löpande:
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    client = RESTClient(api_key)
    aggs = client.get_aggs(ticker, 1, "day", date_str, date_str)

    if not aggs:
        print("⚠️ Ingen data returnerades från Polygon.io")
        return

    print(f"📊 Hämtade {len(aggs)} datapunkter för {ticker} ({date_str})")

    rows = []
    for bar in aggs:
        rows.append(
            {
                "ticker": ticker,
                "timestamp": datetime.fromtimestamp(bar.timestamp / 1000).isoformat(),
                "open": bar.open,
                "close": bar.close,
                "volume": int(bar.volume) if bar.volume is not None else 0,
            }
        )

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "timestamp", "open", "close", "volume"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Skrev {len(rows)} rader till {output_path}")
