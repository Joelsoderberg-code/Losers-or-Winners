#!/usr/bin/env python3
"""
Train Random Forest model and save to GCS for production use.
"""

import joblib
import pandas as pd
from google.cloud import bigquery, storage
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split

# Configuration
BQ_INPUT_TABLE = "winners-or-loosers.stocks_eu.stock_data"
MODEL_PATH = "gs://polygondata/models/randomforest_model.pkl"


def load_data():
    """Load data from BigQuery."""
    bq_client = bigquery.Client()

    query = f"""
    SELECT ticker, timestamp, open, close, volume
    FROM `{BQ_INPUT_TABLE}`
    ORDER BY timestamp DESC
    LIMIT 2000
    """

    df = bq_client.query(query, location="EU").to_dataframe()
    print(f"Loaded {len(df)} rows from BigQuery")
    return df


def prepare_features(df):
    """Prepare features for Random Forest."""
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["return"] = df["close"].pct_change()
    # T+1-mål: nästa dags riktning
    df["target"] = df["return"].shift(-1).gt(0).astype(int)
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

    # Drop NaN values och ta bort sista raden per ticker (saknar T+1)
    df = df.dropna()
    print(f"After feature engineering: {len(df)} rows")
    return df


def train_model(df):
    """Train Random Forest model."""
    feature_cols = [
        "return_lag1",
        "volatility_5d",
        "volatility_20d",
        "momentum_5d",
        "price_above_ma20",
        "ma5_ma20_diff",
        "volume_ratio_20",
        "volume_change_1d",
        "weekday",
        "day_of_month",
    ]

    X = df[feature_cols]
    y = df["target"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

    # Train Random Forest
    rf = RandomForestClassifier(
        n_estimators=100, random_state=42, class_weight="balanced", max_depth=10, min_samples_split=5
    )

    rf.fit(X_train, y_train)

    # Evaluate
    y_pred = rf.predict(X_test)
    y_proba = rf.predict_proba(X_test)[:, 1]

    accuracy = accuracy_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_proba)

    print("Model Performance:")
    print(f"  Accuracy: {accuracy:.3f}")
    print(f"  AUC: {auc:.3f}")
    print("  Feature importance:")
    for feature, importance in zip(feature_cols, rf.feature_importances_, strict=False):
        print(f"    {feature}: {importance:.3f}")

    return rf, feature_cols


def save_model(rf, feature_cols):
    """Save model to GCS."""
    # Save locally first
    local_path = "/tmp/randomforest_model.pkl"
    joblib.dump(rf, local_path)

    # Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket("polygondata")
    blob = bucket.blob("models/randomforest_model.pkl")
    blob.upload_from_filename(local_path)

    print(f"Model saved to: {MODEL_PATH}")

    # Also save feature list for reference
    feature_info = {
        "feature_columns": feature_cols,
        "model_type": "RandomForestClassifier",
        "n_estimators": 100,
        "class_weight": "balanced",
    }

    import json

    feature_path = "/tmp/feature_info.json"
    with open(feature_path, "w") as f:
        json.dump(feature_info, f, indent=2)

    blob = bucket.blob("models/feature_info.json")
    blob.upload_from_filename(feature_path)
    print("Feature info saved to: gs://polygondata/models/feature_info.json")


def main():
    """Main training pipeline."""
    print("Starting Random Forest model training...")

    # Load and prepare data
    df = load_data()
    df = prepare_features(df)

    if len(df) < 100:
        raise ValueError(f"Not enough data for training: {len(df)} rows")

    # Train model
    rf, feature_cols = train_model(df)

    # Save model
    save_model(rf, feature_cols)

    print("Training completed successfully!")


if __name__ == "__main__":
    main()
