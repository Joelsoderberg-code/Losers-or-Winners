import os
import csv
import os
from datetime import datetime

from polygon import RESTClient

from datetime import timedelta

def fetch_data_from_api(api_key: str, ticker: str, output_path: str) -> None:

    # Hämtar gårdagens datum löpande:
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    client = RESTClient(api_key)

    aggs = client.get_aggs(
        ticker,
        1,               # 1-enhet
        "day",           # tidsenhet: dag
        start_date,
        end_date,
    )

    print(f"📊 Hämtade {len(aggs)} datapunkter för {ticker} ({date_str})")

    rows: List[Dict[str, object]] = []
    for bar in aggs:
        rows.append(
            {
                "ticker": ticker,
                "date": datetime.fromtimestamp(bar.timestamp / 1000)
                .date()
                .isoformat(),
                "open": bar.open,
                "close": bar.close,
                "volume": bar.volume,
            }
        )

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["ticker", "date", "open", "close", "volume"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Skrev {len(rows)} rader till {csv_path}")


if __name__ == "__main__":
    fetch_data_from_api()


