from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

with DAG(
    dag_id="predict_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="45 14 * * *",  # 14:45 UTC ≈ 16:45 lokal (sommar) - 5 min efter data-hämtning
    catchup=False,
    tags=["ml"],
) as dag:
    run_scorer = CloudRunExecuteJobOperator(
        task_id="run_model_scorer",
        project_id="winners-or-loosers",
        region="europe-north2",
        job_name="model-scorer",
        # overrides={"container_overrides":[{"name":"predict","env":[
        #     {"name":"BQ_OUTPUT_TABLE","value":"winners-or-loosers.stocks.prediction"},
        # ]}]},
    )
