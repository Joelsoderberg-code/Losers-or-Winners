# prediction_randomforest.py
# Random Forest classifier with enriched feature set (incl. return_lag1 / previous_return)
# Produces predictions, text report, and plots under outputs/

import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

# -------------------- CONFIG --------------------
# BigQuery configuration
BQ_INPUT_TABLE = os.getenv("BQ_INPUT_TABLE", "winners-or-loosers.stocks_eu.stock_data")
BQ_OUTPUT_TABLE = os.getenv("BQ_OUTPUT_TABLE", "winners-or-loosers.stocks_eu.prediction_rf")

OUT_DIR = "outputs"
PRED_CSV = os.path.join(OUT_DIR, "prediction_randomforest.csv")
REPORT_TXT = os.path.join(OUT_DIR, "prediction_randomforest_report.txt")
FEATIMP_CSV = os.path.join(OUT_DIR, "prediction_randomforest_feature_importance.csv")
CM_PNG = os.path.join(OUT_DIR, "prediction_randomforest_confusion_matrix.png")
FI_PNG = os.path.join(OUT_DIR, "prediction_randomforest_feature_importance.png")
DOM_PNG = os.path.join(OUT_DIR, "avg_return_by_day_of_month.png")

os.makedirs(OUT_DIR, exist_ok=True)


# -------------------- LOAD ----------------------
def load_dataframe() -> pd.DataFrame:
    """
    Load data from BigQuery.
    """
    from google.cloud import bigquery

    bq_client = bigquery.Client()

    print(f"Loading data from BigQuery: {BQ_INPUT_TABLE}")
    query = f"""
    SELECT ticker, timestamp, open, close, volume
    FROM `{BQ_INPUT_TABLE}`
    ORDER BY timestamp DESC
    LIMIT 1000
    """

    df = bq_client.query(query, location="EU").to_dataframe()
    print(f"[OK] Loaded {len(df)} rows from BigQuery")
    return df


# -------------------- PREP ----------------------
def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize headers, ensure required columns, and create engineered features.
    Features created (when possible):
      - return (pct change of close)
      - target (1 if return>0 else 0)
      - weekday, day_of_month
      - return_lag1 (yesterday's return)
      - volatility_5d / volatility_20d (rolling std of returns)
      - momentum_5d (5-day % change of close)
      - price_above_ma20, ma5_ma20_diff (moving-average features)
      - volume_ratio_20, volume_change_1d (volume-based features)
    """
    # Normalize headers: strip spaces and lowercase
    df.columns = df.columns.str.strip().str.lower()

    # Ensure timestamp is present and parsed
    if "timestamp" not in df.columns:
        raise KeyError("Missing 'timestamp' column.")
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Ensure return; if missing, compute from open/close
    if "return" not in df.columns:
        if {"open", "close"}.issubset(df.columns):
            df["return"] = df["close"].pct_change()
        else:
            raise KeyError("Missing both 'return' and ('open','close') to compute returns.")

    # Binary target: 1 if today's return > 0
    if "target" not in df.columns:
        df["target"] = (df["return"] > 0).astype(int)

    # Calendar features
    df["weekday"] = df["timestamp"].dt.dayofweek
    df["day_of_month"] = df["timestamp"].dt.day

    # Lagged return (yesterday's return)
    df["return_lag1"] = df["return"].shift(1)

    # Rolling volatility (5d and 20d)
    df["volatility_5d"] = df["return"].rolling(5).std()
    df["volatility_20d"] = df["return"].rolling(20).std()

    # Momentum (5-day % change)
    df["momentum_5d"] = df["close"].pct_change(5)

    # Moving averages
    df["ma5"] = df["close"].rolling(5).mean()
    df["ma20"] = df["close"].rolling(20).mean()
    df["price_above_ma20"] = (df["close"] > df["ma20"]).astype(int)
    df["ma5_ma20_diff"] = df["ma5"] - df["ma20"]

    # Volume features
    df["volume_ma20"] = df["volume"].rolling(20).mean()
    df["volume_ratio_20"] = df["volume"] / df["volume_ma20"]
    df["volume_change_1d"] = df["volume"].pct_change(1)

    return df


# -------------------- TRAIN ---------------------
def train_model(df: pd.DataFrame) -> tuple:
    """
    Train Random Forest classifier with balanced class weights.
    Returns (model, feature_names, train_metrics).
    """
    # Feature columns (exclude target, timestamp, and derived columns)
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

    # Remove rows with NaN (due to rolling calculations)
    df_clean = df.dropna(subset=feature_cols + ["target"])

    if len(df_clean) == 0:
        raise ValueError("No valid data after cleaning NaN values")

    X = df_clean[feature_cols]
    y = df_clean["target"]

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    # Train Random Forest
    rf = RandomForestClassifier(n_estimators=300, max_depth=8, random_state=42, class_weight="balanced")
    rf.fit(X_train, y_train)

    # Predictions and metrics
    y_pred = rf.predict(X_test)
    y_proba = rf.predict_proba(X_test)[:, 1]

    accuracy = accuracy_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_proba)

    print(f"[TRAIN] Accuracy: {accuracy:.3f}, AUC: {auc:.3f}")
    print(f"[TRAIN] Classes: {np.bincount(y)}")

    return rf, feature_cols, {"accuracy": accuracy, "auc": auc, "n_train": len(X_train), "n_test": len(X_test)}


# -------------------- PREDICT -------------------
def predict_and_save(rf, feature_cols, df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate predictions for all data and save results to BigQuery.
    """
    from google.cloud import bigquery

    # Prepare features
    df_clean = df.dropna(subset=feature_cols)
    X = df_clean[feature_cols]

    # Predictions
    preds = rf.predict(X)
    probs = rf.predict_proba(X)[:, 1]

    # Create results DataFrame
    results = df_clean[["timestamp", "ticker"]].copy()
    results["prediction"] = preds
    results["probability"] = probs
    results["actual_return"] = df_clean["return"]
    results["actual_target"] = df_clean["target"]
    results["load_date"] = pd.Timestamp.now().date()

    # Save to CSV locally
    results.to_csv(PRED_CSV, index=False)
    print(f"[PREDICT] Saved {len(results)} predictions to {PRED_CSV}")

    # Save to BigQuery
    bq_client = bigquery.Client()

    # Ensure dataset exists
    try:
        project_id, dataset_id, _ = BQ_OUTPUT_TABLE.split(".")
    except ValueError:
        raise ValueError(f"BQ_OUTPUT_TABLE must be in the form project.dataset.table, got: {BQ_OUTPUT_TABLE}") from None

    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = "EU"
    bq_client.create_dataset(dataset_ref, exists_ok=True)

    # Save to BigQuery - WRITE_TRUNCATE to replace all data
    job = bq_client.load_table_from_dataframe(
        results,
        BQ_OUTPUT_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        location="EU",
    )
    job.result()
    print(f"[PREDICT] Saved {len(results)} predictions to BigQuery -> {BQ_OUTPUT_TABLE}")

    return results


# -------------------- ANALYZE -------------------
def analyze_results(results: pd.DataFrame, rf, feature_cols) -> None:
    """
    Generate analysis reports and visualizations.
    """
    # Text report
    with open(REPORT_TXT, "w") as f:
        f.write("Random Forest Prediction Analysis\n")
        f.write("=" * 40 + "\n\n")

        f.write(f"Total predictions: {len(results)}\n")
        f.write(f"Positive predictions: {results['prediction'].sum()}\n")
        f.write(f"Prediction rate: {results['prediction'].mean():.3f}\n\n")

        # Accuracy on actual data
        if "actual_target" in results.columns:
            accuracy = (results["prediction"] == results["actual_target"]).mean()
            f.write(f"Accuracy vs actual: {accuracy:.3f}\n\n")

        # Feature importance
        f.write("Feature Importance:\n")
        f.write("-" * 20 + "\n")
        for feat, imp in zip(feature_cols, rf.feature_importances_, strict=False):
            f.write(f"{feat}: {imp:.3f}\n")

    print(f"[ANALYZE] Report saved to {REPORT_TXT}")

    # Feature importance CSV
    feat_imp_df = pd.DataFrame({"feature": feature_cols, "importance": rf.feature_importances_}).sort_values(
        "importance", ascending=False
    )
    feat_imp_df.to_csv(FEATIMP_CSV, index=False)
    print(f"[ANALYZE] Feature importance saved to {FEATIMP_CSV}")

    # Visualizations
    plt.style.use("default")

    # Confusion Matrix
    if "actual_target" in results.columns:
        cm = confusion_matrix(results["actual_target"], results["prediction"])
        plt.figure(figsize=(8, 6))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
        plt.title("Confusion Matrix")
        plt.ylabel("Actual")
        plt.xlabel("Predicted")
        plt.tight_layout()
        plt.savefig(CM_PNG, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"[ANALYZE] Confusion matrix saved to {CM_PNG}")

    # Feature Importance Plot
    plt.figure(figsize=(10, 6))
    feat_imp_df.plot(x="feature", y="importance", kind="barh")
    plt.title("Feature Importance")
    plt.xlabel("Importance")
    plt.tight_layout()
    plt.savefig(FI_PNG, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[ANALYZE] Feature importance plot saved to {FI_PNG}")

    # Average return by day of month
    if "day_of_month" in results.columns:
        daily_returns = results.groupby("day_of_month")["actual_return"].mean()
        plt.figure(figsize=(12, 6))
        daily_returns.plot(kind="bar")
        plt.title("Average Return by Day of Month")
        plt.xlabel("Day of Month")
        plt.ylabel("Average Return")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(DOM_PNG, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"[ANALYZE] Day of month analysis saved to {DOM_PNG}")


# -------------------- MAIN ----------------------
def main():
    """Main execution function."""
    print("Starting Random Forest Prediction Pipeline...")

    # Load data
    df = load_dataframe()
    print(f"Loaded {len(df)} rows")

    # Prepare features
    df = prepare_dataframe(df)
    print(f"Prepared features, {len(df)} rows after cleaning")

    # Train model
    rf, feature_cols, metrics = train_model(df)

    # Generate predictions
    results = predict_and_save(rf, feature_cols, df)

    # Analyze results
    analyze_results(results, rf, feature_cols)

    print("\n[SUCCESS] Random Forest pipeline completed!")
    print("Check outputs/ directory for results")


if __name__ == "__main__":
    main()
