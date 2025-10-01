# plugins/predict.py
import os
from datetime import datetime

import joblib
from google.cloud import bigquery

# ---------------- CONFIG ----------------
MODEL_PATH = "/home/airflow/gcs/data/models/models_logreg_model.pkl"
BQ_INPUT_TABLE = "winners-or-loosers.stocks.stock_data"
BQ_OUTPUT_TABLE = "winners-or-loosers.stocks.prediction"
LOCAL_CSV_PATH = "/home/airflow/gcs/data/outputs/predictions.csv"

bq_client = bigquery.Client()


def predict_and_save():
    print(f"Loading Pickle model from: {MODEL_PATH}")
    model = joblib.load(MODEL_PATH)

    print(f"Loading data from BigQuery: {BQ_INPUT_TABLE}")
    df = bq_client.query(f"SELECT * FROM `{BQ_INPUT_TABLE}`").to_dataframe()
    print(f"Rows fetched: {len(df)}")

    # Feature engineering
    df["change"] = df["close"] - df["open"]

    # Predict
    X = df[["change"]]
    preds = model.predict(X)
    probs = model.predict_proba(X)[:, 1]

    df["prediction"] = preds
    df["probability"] = probs
    df["load_date"] = datetime.utcnow().date()

    # Save locally (GCS bucket mounted in Composer)
    os.makedirs(os.path.dirname(LOCAL_CSV_PATH), exist_ok=True)
    df.to_csv(LOCAL_CSV_PATH, index=False)
    print(f"[OK] Saved local CSV -> {LOCAL_CSV_PATH}")

    # Save to BigQuery
    job = bq_client.load_table_from_dataframe(
        df, BQ_OUTPUT_TABLE, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"[OK] Appended to BigQuery -> {BQ_OUTPUT_TABLE}")
