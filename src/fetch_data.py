import os
import csv
from datetime import datetime
from typing import List, Dict

from dotenv import load_dotenv
from polygon import RESTClient

from datetime import datetime, timedelta

def fetch_data_from_api(api_key: str, ticker: str, output_path: str) -> None:
    from polygon import RESTClient
    import csv
    from datetime import datetime, timedelta
    import os

    # Hämta gårdagens datum
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
        rows.append({
            "ticker": ticker,
            "timestamp": datetime.fromtimestamp(bar.timestamp / 1000).isoformat(),
            "open": bar.open,
            "close": bar.close,
            "volume": bar.volume,
        })

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "timestamp", "open", "close", "volume"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Skrev {len(rows)} rader till {output_path}")