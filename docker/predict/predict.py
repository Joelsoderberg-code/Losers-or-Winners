# plugins/predict.py
# ---------------- CONFIG ----------------
import os
from datetime import datetime

import fsspec
import joblib
from google.cloud import bigquery, storage

MODEL_PATH = "gs://polygondata/models/randomforest_model.pkl"
BQ_INPUT_TABLE = os.getenv("BQ_INPUT_TABLE", "winners-or-loosers.stocks_eu.stock_data")
BQ_OUTPUT_TABLE = os.getenv("BQ_OUTPUT_TABLE", "winners-or-loosers.stocks_eu.prediction")
LOCAL_CSV_PATH = "gs://polygondata/data/outputs/predictions.csv"

bq_client = bigquery.Client()


def predict_and_save():
    print(f"Loading Pickle model from: {MODEL_PATH}")
    with fsspec.open(MODEL_PATH, "rb") as f:
        model = joblib.load(f)

    print(f"Loading data from BigQuery: {BQ_INPUT_TABLE}")
    import pandas as pd

    df = bq_client.query(
        f"SELECT ticker, timestamp, open, close, volume FROM `{BQ_INPUT_TABLE}` ORDER BY timestamp DESC LIMIT 1000",
        location="EU",
    ).to_dataframe()
    print(f"Rows fetched: {len(df)}")

    # Feature engineering for Random Forest
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["return"] = df["close"].pct_change()
    df["return_lag1"] = df["return"].shift(1)
    df["volatility_5d"] = df["return"].rolling(window=5).std()
    df["volatility_20d"] = df["return"].rolling(window=20).std()
    df["momentum_5d"] = df["close"].pct_change(periods=5)
    df["ma20"] = df["close"].rolling(window=20).mean()
    df["ma5"] = df["close"].rolling(window=5).mean()
    df["price_above_ma20"] = (df["close"] > df["ma20"]).astype(int)
    df["ma5_ma20_diff"] = df["ma5"] - df["ma20"]
    df["volume_ratio_20"] = df["volume"] / df["volume"].rolling(window=20).mean()
    df["volume_change_1d"] = df["volume"].pct_change()
    df["weekday"] = df["timestamp"].dt.dayofweek
    df["day_of_month"] = df["timestamp"].dt.day

    # Drop NaN values
    df = df.dropna()

    # Prepare features for Random Forest (same order as training)
    feature_cols = [
        "return",
        "weekday",
        "day_of_month",
        "return_lag1",
        "volatility_5d",
        "volatility_20d",
        "momentum_5d",
        "price_above_ma20",
        "ma5_ma20_diff",
        "volume_ratio_20",
        "volume_change_1d",
    ]

    X = df[feature_cols]
    preds = model.predict(X)
    probs = model.predict_proba(X)[:, 1]

    df["prediction"] = preds
    df["probability"] = probs
    df["load_date"] = datetime.utcnow().date()

    # Save to GCS directly (Cloud Run compatible via gcsfs)
    df.to_csv(LOCAL_CSV_PATH, index=False)
    print(f"[OK] Saved local CSV -> {LOCAL_CSV_PATH}")

    # Explicit upload fallback to ensure file lands in GCS
    tmp_path = "/tmp/predictions.csv"
    df.to_csv(tmp_path, index=False)
    storage.Client().bucket("polygondata").blob("data/outputs/predictions.csv").upload_from_filename(tmp_path)
    print("[OK] Uploaded CSV -> gs://polygondata/data/outputs/predictions.csv")

    # Ensure dataset exists (auto-create if missing) and then save to BigQuery
    try:
        project_id, dataset_id, _ = BQ_OUTPUT_TABLE.split(".")
    except ValueError:
        raise ValueError(f"BQ_OUTPUT_TABLE must be in the form project.dataset.table, got: {BQ_OUTPUT_TABLE}") from None
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    # Match dataset location (EU)
    dataset_ref.location = "EU"
    bq_client.create_dataset(dataset_ref, exists_ok=True)

    # Save to BigQuery (auto-creates table if needed) - WRITE_TRUNCATE to replace all data
    job = bq_client.load_table_from_dataframe(
        df,
        BQ_OUTPUT_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        location="EU",
    )
    job.result()
    print(f"[OK] Replaced data in BigQuery -> {BQ_OUTPUT_TABLE}")


if __name__ == "__main__":
    predict_and_save()
