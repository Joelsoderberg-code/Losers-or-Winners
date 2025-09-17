import os

from google.cloud import bigquery


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    try:
        client.get_dataset(dataset_id)
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_id), exists_ok=True)


def load_csv_to_bq(
    csv_path: str,
    project_id: str,
    dataset_name: str,
    table_name: str,
    write_disposition: str = "WRITE_APPEND",
) -> None:
    client = bigquery.Client(project=project_id)
    dataset_id = f"{project_id}.{dataset_name}"
    table_id = f"{dataset_id}.{table_name}"

    ensure_dataset(client, dataset_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=write_disposition,
    )

    with open(csv_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)

    result = load_job.result()
    table = client.get_table(table_id)
    print(
        f"Laddning klar: {result.output_rows} rader till {table_id}. Totala rader nu: {table.num_rows}"
    )


if __name__ == "__main__":
    # Konfiguration via miljövariabler eller standardvärden
    csv_path = os.getenv("CSV_PATH", "/home/joel/Losers-or-Winners/data/stock_data.csv")
    project_id = os.getenv("GCP_PROJECT_ID", "your-project-id")
    dataset_name = os.getenv("BQ_DATASET", "stocks")
    table_name = os.getenv("BQ_TABLE", "stock_data")
    write_disposition = os.getenv("BQ_WRITE_DISPOSITION", "WRITE_APPEND")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV hittas inte: {csv_path}")

    print(
        f"Laddar {csv_path} till {project_id}:{dataset_name}.{table_name} (mode={write_disposition})"
    )
    print("Tips: Sätt autentisering via GOOGLE_APPLICATION_CREDENTIALS")
    print("eller kör 'gcloud auth application-default login' innan.")
    load_csv_to_bq(csv_path, project_id, dataset_name, table_name, write_disposition)
