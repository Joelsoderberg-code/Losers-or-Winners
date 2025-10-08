import configparser
import os

from dotenv import load_dotenv


def _load_project_env() -> None:
    """Läs in env från projektets config-fil om den finns.

    Ordning (första som hittas vinner, utan att skriva över befintliga env):
    - ENV_FILE (explicit path)
    - ../config/.env (relativt denna fil)
    - ../.env (repo-root gissning)
    - .env (cwd)
    """
    explicit_path = os.getenv("ENV_FILE")
    if explicit_path and os.path.exists(explicit_path):
        load_dotenv(explicit_path, override=False)
        return

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
            pass

    load_dotenv(override=False)


def _load_project_config() -> dict:
    """Läs delbar konfig från config/config.ini om den finns.

    Returnerar en platt dict från [default]-sektionen. Saknas fil → {}.
    """
    config = configparser.ConfigParser()
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


from google.cloud import bigquery  # noqa: E402


def save_data_to_bigquery() -> None:
    """Läs en CSV och ladda den till BigQuery med explicit schema.

    Miljövariabler (med standardvärden):
    - CSV_PATH: sökväg till CSV (lokalt default /home/joel/...,
      i Composer sätt via Variable till /home/airflow/gcs/data/stock_data.csv)
    - GCP_PROJECT_ID: GCP-projekt (annars ADC default)
    - BQ_DATASET: datasetnamn (default "stocks")
    - BQ_TABLE: tabellnamn (default "stock_data")
    - BQ_WRITE_DISPOSITION: WRITE_APPEND (daglig), ev. WRITE_TRUNCATE vid schemafix
    """

    _load_project_env()
    cfg = _load_project_config()

    # Konfigurerbara via .env eller Airflow Variables
    csv_path = os.getenv("CSV_PATH") or cfg.get("CSV_PATH") or "/home/joel/Losers-or-Winners/data/stock_data.csv"
    project_id = os.getenv("GCP_PROJECT_ID") or cfg.get("GCP_PROJECT_ID")
    dataset_name = os.getenv("BQ_DATASET") or cfg.get("BQ_DATASET") or "stocks"
    table_name = os.getenv("BQ_TABLE") or cfg.get("BQ_TABLE") or "stock_data"
    # staging-tabellnamn kan konfigureras, annars <table>_staging
    staging_table_name = os.getenv("BQ_STAGING_TABLE") or cfg.get("BQ_STAGING_TABLE") or f"{table_name}_staging"
    # write_disposition kept for compatibility in cfg; not used with staging+merge

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV hittas inte: {csv_path}")

    client = bigquery.Client(project=project_id) if project_id else bigquery.Client()

    dataset_id = f"{client.project}.{dataset_name}"
    table_id = f"{dataset_id}.{table_name}"
    staging_table_id = f"{dataset_id}.{staging_table_name}"

    # Säkerställ datasetet finns
    try:
        client.get_dataset(dataset_id)
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_id), exists_ok=True)

    # Robust schema: undvik autodetect-krockar vid APPEND
    schema = [
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "FLOAT"),
    ]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=schema,
        # Till staging använder vi alltid WRITE_TRUNCATE (idempotent MERGE efteråt)
        write_disposition="WRITE_TRUNCATE",
    )

    # 1) Ladda till staging-tabell
    with open(csv_path, "rb") as f:
        load_job = client.load_table_from_file(f, staging_table_id, job_config=job_config)

    result = load_job.result()
    # 2) MERGE staging -> target på nyckel (ticker, DATE(timestamp))
    merge_sql = f"""
        MERGE `{table_id}` T
        USING (
          SELECT
            ticker,
            TIMESTAMP_TRUNC(timestamp, DAY) AS ts_day,
            ANY_VALUE(open) AS open,
            ANY_VALUE(close) AS close,
            ANY_VALUE(volume) AS volume
          FROM `{staging_table_id}`
          GROUP BY ticker, ts_day
        ) S
        ON T.ticker = S.ticker AND DATE(T.timestamp) = DATE(S.ts_day)
        WHEN MATCHED THEN UPDATE SET
          T.timestamp = S.ts_day,
          T.open = S.open,
          T.close = S.close,
          T.volume = S.volume
        WHEN NOT MATCHED THEN INSERT (ticker, timestamp, open, close, volume)
        VALUES (S.ticker, S.ts_day, S.open, S.close, S.volume)
    """

    client.query(merge_sql).result()

    table = client.get_table(table_id)
    print(
        "Laddning klar till staging: "
        f"{result.output_rows} rader. "
        f"MERGE -> {table_id}. "
        f"Totala rader nu: {table.num_rows}"
    )


if __name__ == "__main__":
    save_data_to_bigquery()
