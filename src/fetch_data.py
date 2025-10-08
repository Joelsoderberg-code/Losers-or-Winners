"""Hämtar dagliga OHLCV-data från Polygon API och skriver till CSV.

Funktionalitet:
- Läser API-nyckel och parametrar från argument eller miljövariabler/.env
- Bestämmer datumintervall: START/END, BACKFILL_DAYS eller default "igår"
- Skriver CSV med kolumner: ticker, timestamp, open, close, volume

Användning (via DAG eller direkt):
    fetch_data_from_api()
"""

# Importerar alla nödvändiga bibliotek:
import configparser
import csv
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from polygon import RESTClient


def _load_project_env() -> None:
    """Load env vars from project config if available.

    Search order (first found wins, no override of existing env):
    - ENV_FILE (explicit path)
    - ../config/.env (relative to this file)
    - ../.env (repo root relative guess)
    - .env (cwd)
    """
    # 1) Explicit path via ENV_FILE
    explicit_path = os.getenv("ENV_FILE")
    if explicit_path and os.path.exists(explicit_path):
        load_dotenv(explicit_path, override=False)
        return

    # 2) Relative to this file (Composer places src under dags/src)
    script_dir = os.path.dirname(__file__)
    candidates = [
        os.path.join(script_dir, "..", "config", ".env"),
        os.path.join(script_dir, "..", ".env"),
        os.path.join(os.getcwd(), ".env"),
    ]
    for candidate in candidates:
        try:
            if os.path.exists(candidate):
                load_dotenv(candidate, override=False)
                return
        except Exception:
            # Best-effort; fall through to default load
            pass

    # 3) Fallback to default loader (cwd)
    load_dotenv(override=False)


def _load_project_config() -> dict:
    """Load shared, non-secret configuration from config/config.ini if present.

    Returns a flat dict of values from [default] section. Missing file → {}.
    """
    config = configparser.ConfigParser()
    # Resolve path candidates relative to this file / repo root / cwd
    script_dir = os.path.dirname(__file__)
    candidates = [
        os.getenv("CONFIG_FILE"),
        os.path.join(script_dir, "..", "config", "config.ini"),
        os.path.join(script_dir, "..", "config.ini"),
        os.path.join(os.getcwd(), "config", "config.ini"),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        try:
            if os.path.exists(candidate):
                config.read(candidate)
                break
        except Exception:
            continue
    if "default" in config:
        return {k.upper(): v for k, v in config["default"].items()}
    return {}


def fetch_data_from_api(
    api_key: str | None = None,
    ticker: str | None = None,
    output_path: str | None = None,
) -> None:
    """Hämta aggregerade dagsdata och skriv till CSV.

    Parametrar prioriteras i ordning: funktionsargument → miljövariabler/.env → defaults.
    """
    # Läs in projektkonfig först (utan att skriva över redan satta env)
    _load_project_env()
    cfg = _load_project_config()
    api_key = api_key or os.getenv("POLYGON_API_KEY") or cfg.get("POLYGON_API_KEY")
    if not api_key:
        raise ValueError("POLYGON_API_KEY saknas i .env/miljön och gavs inte som argument")

    ticker = ticker or os.getenv("TICKER") or cfg.get("TICKER") or "SPY"

    if output_path is None:
        # Lokalt defaultar vi till projektmappen; i Composer skrivs detta
        # över via Airflow Variables till /home/airflow/gcs/data.
        output_dir = os.getenv("OUTPUT_DIR") or cfg.get("OUTPUT_DIR") or "/home/joel/Losers-or-Winners/data"
        output_file = os.getenv("OUTPUT_FILE") or cfg.get("OUTPUT_FILE") or "stock_data.csv"
        output_path = os.path.join(output_dir, output_file)

    # Bestäm intervall:
    # 1) START_DATE/END_DATE om båda finns
    # 2) BACKFILL_DAYS (t.ex. 365) → från (igår - BACKFILL_DAYS + 1) till igår
    # 3) annars: endast igår
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    start_env = os.getenv("START_DATE") or cfg.get("START_DATE")
    end_env = os.getenv("END_DATE") or cfg.get("END_DATE")
    backfill_days_str = os.getenv("BACKFILL_DAYS") or cfg.get("BACKFILL_DAYS") or "0"
    try:
        backfill_days = int(backfill_days_str)
    except ValueError:
        backfill_days = 0

    if start_env and end_env:
        start_date = start_env
        end_date = end_env
    elif backfill_days > 0:
        start_date = (yesterday - timedelta(days=backfill_days - 1)).isoformat()
        end_date = yesterday.isoformat()
    else:
        start_date = end_date = yesterday.isoformat()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    client = RESTClient(api_key)
    # Försök hämta fler rader och i stigande ordning
    aggs = client.get_aggs(ticker, 1, "day", start_date, end_date, limit=50000, sort="asc")

    if not aggs:
        print("⚠️ Ingen data returnerades från Polygon.io")
        return

    print(f"📊 Hämtade {len(aggs)} datapunkter för {ticker} ({start_date}..{end_date})")

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
