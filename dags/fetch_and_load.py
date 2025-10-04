"""Airflow DAG som hamtar daglig borsdata fran Polygon och laddar till BigQuery.

- Hamtninig: defaultar till "igar" (UTC) om START_DATE/END_DATE inte ar satta
- Lagring: laddar en CSV till BigQuery med explicit schema (open/close/volume som FLOAT)
- Miljoer: fungerar bade lokalt och i Composer; paths styrs via Airflow Variables
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

# Importera dina egna funktioner
from src.fetch_data import fetch_data_from_api
from src.save_to_bigquery import save_data_to_bigquery

default_args = {
    "owner": "arvid",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}


with DAG(
    dag_id="fetch_and_load_pipeline",
    default_args=default_args,
    schedule_interval="40 14 * * *",  # 14:40 UTC ≈ 16:40 lokal (sommar)
    catchup=False,
    tags=["data_pipeline"],
) as dag:

    def fetch_wrapper():
        """Satt miljobariabler och hamta data fran Polygon.

        Logik:
        - Om BACKFILL_DONE=True: hämta bara igår (daglig uppdatering)
        - Om BACKFILL_DONE=False: hämta historisk data (backfill) och sätt BACKFILL_DONE=True
        """
        # Läs från .env först, fallback till Airflow Variables
        load_dotenv()

        # API-nyckel: .env först, sedan Airflow Variables
        api_key = os.getenv("POLYGON_API_KEY") or Variable.get("POLYGON_API_KEY", default_var=None)
        if not api_key:
            raise ValueError("POLYGON_API_KEY saknas i både .env och Airflow Variables")
        os.environ["POLYGON_API_KEY"] = api_key

        # Ticker
        ticker_var = Variable.get("TICKER", default_var=None)
        if ticker_var:
            os.environ["TICKER"] = ticker_var
        else:
            os.environ["TICKER"] = "SPY"

        # Kolla om backfill redan är gjort
        backfill_done = Variable.get("BACKFILL_DONE", default_var="False")

        if backfill_done == "True":
            # Daglig uppdatering: hämta bara igår
            yesterday = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
            os.environ["START_DATE"] = yesterday
            os.environ["END_DATE"] = yesterday
            print(f"[DAGLIG] Hämtar data för {yesterday}")
        else:
            # Backfill: hämta historisk data
            start_var = Variable.get("START_DATE", default_var=None)
            end_var = Variable.get("END_DATE", default_var=None)
            if start_var and end_var:
                os.environ["START_DATE"] = start_var
                os.environ["END_DATE"] = end_var
                print(f"[BACKFILL] Hämtar historisk data från {start_var} till {end_var}")
            else:
                # Default backfill: senaste året
                one_year_ago = (datetime.utcnow().date() - timedelta(days=365)).isoformat()
                yesterday = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
                os.environ["START_DATE"] = one_year_ago
                os.environ["END_DATE"] = yesterday
                print(f"[BACKFILL] Hämtar senaste året: {one_year_ago} till {yesterday}")

        os.environ["OUTPUT_DIR"] = Variable.get("OUTPUT_DIR", default_var="/home/airflow/gcs/data")
        os.environ["OUTPUT_FILE"] = Variable.get("OUTPUT_FILE", default_var="stock_data.csv")
        fetch_data_from_api()

    def load_wrapper():
        """Ladda CSV till BigQuery med explicit schema.

        Viktiga variabler:
        - CSV_PATH: sokvag till CSV (lokalt eller Composer-path)
        - GCP_PROJECT_ID: GCP-projekt; annars ADC default
        - BQ_DATASET/BQ_TABLE: maltabell (partitionerad pa DATE(timestamp))
        - BQ_WRITE_DISPOSITION: WRITE_APPEND (daglig), ev. WRITE_TRUNCATE for engangssfix
        """
        # Läs parametrar från Airflow Variables (med defaultvärden)
        os.environ["CSV_PATH"] = Variable.get("CSV_PATH", default_var="/home/airflow/gcs/data/stock_data.csv")
        # GCP-projekt kan komma från ADC; sätt bara om det finns
        gcp_project = Variable.get("GCP_PROJECT_ID", default_var=None)
        if gcp_project:
            os.environ["GCP_PROJECT_ID"] = gcp_project
        os.environ["BQ_DATASET"] = Variable.get("BQ_DATASET", default_var="stocks")
        os.environ["BQ_TABLE"] = Variable.get("BQ_TABLE", default_var="stock_data")
        os.environ["BQ_WRITE_DISPOSITION"] = Variable.get("BQ_WRITE_DISPOSITION", default_var="WRITE_APPEND")
        # Autentisering: antingen ADC eller servicekonto via fil
        sa_path = Variable.get("GOOGLE_APPLICATION_CREDENTIALS", default_var=None)
        if sa_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path

        # Ladda data till BigQuery
        save_data_to_bigquery()

        # Efter första backfill: sätt BACKFILL_DONE=True
        backfill_done = Variable.get("BACKFILL_DONE", default_var="False")
        if backfill_done == "False":
            Variable.set("BACKFILL_DONE", "True")
            print("[BACKFILL] Satt BACKFILL_DONE=True - nästa körning blir daglig uppdatering")

    fetch_task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_wrapper,
    )

    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_wrapper,
    )

    fetch_task >> load_task
