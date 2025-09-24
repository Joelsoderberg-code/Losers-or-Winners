import os

from dotenv import load_dotenv
from google.cloud import bigquery


def save_data_to_bigquery() -> None:
    """
    Läser CSV och laddar till BigQuery.

    Miljövariabler (med standardvärden):
    - CSV_PATH (default: /home/joel/Losers-or-Winners/data/stock_data.csv)
    - GCP_PROJECT_ID (krävs eller konfigurerad via ADC)
    - BQ_DATASET (default: stocks)
    - BQ_TABLE (default: stock_data)
    - BQ_WRITE_DISPOSITION (default: WRITE_APPEND)
    """

    load_dotenv()

    # Konfigurerbara via .env eller Airflow Variables
    csv_path = os.getenv("CSV_PATH", "/home/joel/Losers-or-Winners/data/stock_data.csv")
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_name = os.getenv("BQ_DATASET", "stocks")
    table_name = os.getenv("BQ_TABLE", "stock_data")
    write_disposition = os.getenv("BQ_WRITE_DISPOSITION", "WRITE_APPEND")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV hittas inte: {csv_path}")

    client = bigquery.Client(project=project_id) if project_id else bigquery.Client()

    dataset_id = f"{client.project}.{dataset_name}"
    table_id = f"{dataset_id}.{table_name}"

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
        write_disposition=write_disposition,
    )

    with open(csv_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)

    result = load_job.result()
    table = client.get_table(table_id)
    print(f"Laddning klar: {result.output_rows} rader till {table_id}. Totala rader nu: {table.num_rows}")


if __name__ == "__main__":
    save_data_to_bigquery()
