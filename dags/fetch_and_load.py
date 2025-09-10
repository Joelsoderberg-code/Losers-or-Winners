from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
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
    schedule_interval=" 0 3 * * *",
    catchup=False,
    tags=["data_pipeline"],
) as dag:

    def fetch_wrapper():
        # Läs från .env först, fallback till Airflow Variables
        load_dotenv()
        
        # API-nyckel: .env först, sedan Airflow Variables
        api_key = os.getenv("POLYGON_API_KEY") or Variable.get("POLYGON_API_KEY", default_var=None)
        if not api_key:
            raise ValueError("POLYGON_API_KEY saknas i både .env och Airflow Variables")
        os.environ["POLYGON_API_KEY"] = api_key
        
        # Övriga parametrar: Läs TICKER endast om den finns som Variable.
        # Om den inte finns låter vi koden använda sin default (I:SPX).
        ticker_var = Variable.get("TICKER", default_var=None)
        if ticker_var:
            os.environ["TICKER"] = ticker_var
        os.environ["START_DATE"] = Variable.get("START_DATE", default_var="2025-09-01")
        os.environ["END_DATE"] = Variable.get("END_DATE", default_var="2025-09-03")
        os.environ["OUTPUT_DIR"] = Variable.get(
            "OUTPUT_DIR", default_var="/home/joel/Losers-or-Winners/data"
        )
        os.environ["OUTPUT_FILE"] = Variable.get("OUTPUT_FILE", default_var="stock_data.csv")
        fetch_data_from_api()

    def load_wrapper():
        # Läs parametrar från Airflow Variables (med defaultvärden)
        os.environ["CSV_PATH"] = Variable.get(
            "CSV_PATH", default_var="/home/joel/Losers-or-Winners/data/stock_data.csv"
        )
        # GCP-projekt kan komma från ADC; sätt bara om det finns
        gcp_project = Variable.get("GCP_PROJECT_ID", default_var=None)
        if gcp_project:
            os.environ["GCP_PROJECT_ID"] = gcp_project
        os.environ["BQ_DATASET"] = Variable.get("BQ_DATASET", default_var="stocks")
        os.environ["BQ_TABLE"] = Variable.get("BQ_TABLE", default_var="stock_data")
        os.environ["BQ_WRITE_DISPOSITION"] = Variable.get(
            "BQ_WRITE_DISPOSITION", default_var="WRITE_APPEND"
        )
        # Autentisering: antingen ADC eller servicekonto via fil
        sa_path = Variable.get("GOOGLE_APPLICATION_CREDENTIALS", default_var=None)
        if sa_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
        save_data_to_bigquery()

    fetch_task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_wrapper,
    )

    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_wrapper,
    )

    fetch_task >> load_task


