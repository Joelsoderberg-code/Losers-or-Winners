import os
import csv
from datetime import datetime
from typing import List, Dict

from dotenv import load_dotenv
from polygon import RESTClient


def fetch_data_from_api() -> None:
    """
    Hämtar dagliga aggregerade priser från Polygon och sparar till CSV.

    Konfiguration via miljövariabler (med standardvärden):
    - POLYGON_API_KEY (krävs)
    - TICKER (default: AAPL)
    - START_DATE (default: 2025-09-01)
    - END_DATE (default: 2025-09-03)
    - OUTPUT_DIR (default: /home/joel/Losers-or-Winners/data)
    - OUTPUT_FILE (default: stock_data.csv)
    """

    load_dotenv()

    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("POLYGON_API_KEY saknas i miljön")

    # Standard: använd ETF:en SPY (tillgänglig på de flesta planer)
    ticker = os.getenv("TICKER", "SPY")
    start_date = os.getenv("START_DATE", "2025-09-01")
    end_date = os.getenv("END_DATE", "2025-09-03")

    output_dir = os.getenv("OUTPUT_DIR", "/home/joel/Losers-or-Winners/data")
    output_file = os.getenv("OUTPUT_FILE", "stock_data.csv")
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, output_file)

    client = RESTClient(api_key)

    aggs = client.get_aggs(
        ticker,
        1,               # 1-enhet
        "day",           # tidsenhet: dag
        start_date,
        end_date,
    )

    print(f"Hämtade {len(aggs)} datapunkter för {ticker} ({start_date}..{end_date})")

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


