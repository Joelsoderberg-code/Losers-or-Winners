# Losers-or-Winners Data Platform - System Flowchart

## 🏗️ Hela Systemarkitekturen

### Quick Map (Mermaid – aktuellt läge)

```mermaid
flowchart LR
  subgraph Airflow[Apache Airflow (Composer)]
    A1[fetch_and_load_pipeline\nCron: 06:30 UTC]
    A2[predict_pipeline\nCron: 06:35 UTC]
  end

  subgraph Ingestion[Ingestion]
    I1[Polygon API]
    I2[fetch_data.py\n.env + config.ini (Variables fallback)]
    I3[save_to_bigquery.py\nStaging + MERGE (idempotent)\n→ stocks_eu.stock_data]
  end

  subgraph ML[ML]
    M1[train_randomforest.py\nT+1 target, 10 features]
    M2[(GCS) models/randomforest_model.pkl]
  end

  subgraph Serving[Batch Predict (Cloud Run Job)]
    S1[model-scorer (v16)\npredict.py\n→ WRITE_TRUNCATE]
    S2[(BQ) stocks_eu.prediction]
  end

  subgraph Storage[Storage & Viz]
    GCS[(GCS) data/outputs/predictions.csv]
    VZ[Looker Studio / SQL]
  end

  %% Flöden
  A1 -->|trigger| I2 --> I3 --> SDD[(BQ) stock_data]
  I1 --> I2
  SDD --> M1 --> M2
  A2 -->|execute Job| S1
  M2 --> S1
  SDD --> S1
  S1 -->|CSV| GCS
  S1 --> S2
  S2 --> VZ
```

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           LOSERS-OR-WINNERS DATA PLATFORM                      │
│                                                                                 │
│  📊 DATA INGESTION    🔄 ML PIPELINE    🚀 DEPLOYMENT    📈 MONITORING         │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 📊 DATA INGESTION FLOW

### 1. Airflow DAG: `fetch_and_load_pipeline`
```
┌─────────────────────────────────────────────────────────────────┐
│                    AIRFLOW SCHEDULER                           │
│                   (Körs dagligen 06:30 UTC)                   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                DAG: fetch_and_load_pipeline                    │
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌──────────────┐ │
│  │   fetch_wrapper │───▶│   load_wrapper  │───▶│  BACKFILL    │ │
│  │                 │    │                 │    │  DONE=True   │ │
│  └─────────────────┘    └─────────────────┘    └──────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BACKFILL LOGIC                              │
│                                                                 │
│  IF BACKFILL_DONE=False:                                       │
│    ├─ Hämta historisk data (1 år bakåt)                        │
│    └─ Sätt BACKFILL_DONE=True                                  │
│                                                                 │
│  IF BACKFILL_DONE=True:                                        │
│    └─ Hämta bara igår (daglig uppdatering)                     │
└─────────────────────────────────────────────────────────────────┘
```

### 2. Data Fetching: `src/fetch_data.py`
```
┌─────────────────────────────────────────────────────────────────┐
│                    POLYGON API                                 │
│              (Market Data Provider)                            │
└─────────────────────────────────────────────────────────────────┘
                                │
                                │ API Call
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                fetch_data_from_api()                           │
│                                                                 │
│  Input:  START_DATE, END_DATE, TICKER, API_KEY                 │
│  Process:                                                        │
│    ├─ Polygon RESTClient                                       │
│    ├─ OHLCV data hämtning                                      │
│    └─ CSV skrivning                                            │
│  Output: stock_data.csv                                        │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LOCAL/GCS STORAGE                           │
│              /home/airflow/gcs/data/stock_data.csv             │
└─────────────────────────────────────────────────────────────────┘
```

### 3. Data Loading: `src/save_to_bigquery.py`
```
┌─────────────────────────────────────────────────────────────────┐
│                save_data_to_bigquery()                         │
│                                                                 │
│  Input:  CSV file (stock_data.csv)                            │
│  Process:                                                        │
│    ├─ Läsa CSV med pandas                                      │
│    ├─ Schema validering                                        │
│    ├─ Datatypskonvertering (FLOAT för OHLCV)                   │
│    └─ BigQuery load job                                        │
│  Output: BigQuery table                                        │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BIGQUERY DATABASE                           │
│                                                                 │
│  Project: winners-or-loosers                                   │
│  Dataset: stocks_eu                                            │
│  Table: stock_data                                             │
│                                                                 │
│  Schema:                                                       │
│    ├─ ticker (STRING)                                          │
│    ├─ timestamp (TIMESTAMP)                                    │
│    ├─ open (FLOAT)                                             │
│    ├─ close (FLOAT)                                            │
│    └─ volume (FLOAT)                                           │
└─────────────────────────────────────────────────────────────────┘
```

## 🤖 ML PIPELINE FLOW

### 4. Model Training: `src/train_randomforest.py`
```
┌─────────────────────────────────────────────────────────────────┐
│                    BIGQUERY DATA                               │
│              winners-or-loosers.stocks_eu.stock_data           │
└─────────────────────────────────────────────────────────────────┘
                                │
                                │ SQL Query
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                train_randomforest.py                           │
│                                                                 │
│  Data Processing:                                              │
│    ├─ Ladda senaste rader (tillräckligt för T+1)              │
│    ├─ Feature Engineering (10 features):                      │
│    │   ├─ weekday, day_of_month                               │
│    │   ├─ return_lag1, volatility_5d, volatility_20d          │
│    │   ├─ momentum_5d, price_above_ma20                       │
│    │   ├─ ma5_ma20_diff, volume_ratio_20                      │
│    │   └─ volume_change_1d                                    │
│    └─ Data cleaning (dropna)                                  │
│                                                                 │
│  Model Training:                                               │
│    ├─ RandomForestClassifier                                  │
│    ├─ n_estimators=300, max_depth=8                           │
│    ├─ class_weight="balanced"                                 │
│    └─ train_test_split (80/20)                                │
│                                                                 │
│  Target:                                                       │
│    └─ T+1 (nästa dags upp/ner): target = (return.shift(-1)>0)  │
│  Evaluation:                                                   │
│    ├─ Accuracy: …                                             │
│    └─ AUC: …                                                  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MODEL PERSISTENCE                           │
│                                                                 │
│  Local: randomforest_model.pkl                                │
│  GCS: gs://polygondata/models/randomforest_model.pkl          │
│  Size: 723KB                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 5. Prediction DAG: `dags/predict_dag.py`
```
┌─────────────────────────────────────────────────────────────────┐
│                    AIRFLOW SCHEDULER                           │
│                   (Körs dagligen 06:35 UTC)                   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                DAG: predict_pipeline                           │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │        CloudRunExecuteJobOperator                          │ │
│  │                                                             │ │
│  │  Project: winners-or-loosers                               │ │
│  │  Region: europe-north2                                     │ │
│  │  Job: model-scorer                                         │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GOOGLE CLOUD RUN                            │
│                                                                 │
│  Job: model-scorer                                             │
│  Container: europe-north2-docker.pkg.dev/.../model-scorer:v16 │
│  Resources: CPU/Memory auto-scaling                            │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 DEPLOYMENT FLOW

### 6. Docker Container: `docker/predict/`
```
┌─────────────────────────────────────────────────────────────────┐
│                    DOCKER BUILD                                │
│                                                                 │
│  Base: python:3.11-slim                                        │
│  Dependencies:                                                 │
│    ├─ joblib, pandas, numpy                                    │
│    ├─ scikit-learn, db-dtypes                                  │
│    ├─ google-cloud-bigquery, gcsfs                            │
│    └─ pyarrow                                                  │
│                                                                 │
│  Files:                                                        │
│    ├─ Dockerfile                                               │
│    ├─ predict.py (production script)                          │
│    └─ requirements.txt                                         │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GOOGLE ARTIFACT REGISTRY                    │
│                                                                 │
│  Registry: europe-north2-docker.pkg.dev                       │
│  Repository: winners-or-loosers/predict                        │
│  Image: model-scorer:v16                                       │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GOOGLE CLOUD RUN                            │
│                                                                 │
│  Service: model-scorer                                         │
│  URL: https://model-scorer-xxx-uc.a.run.app                   │
│  Trigger: Airflow DAG (scheduled)                             │
└─────────────────────────────────────────────────────────────────┘
```

### 7. Production Prediction: `docker/predict/predict.py`
```
┌─────────────────────────────────────────────────────────────────┐
│                    MODEL LOADING                               │
│                                                                 │
│  Source: gs://polygondata/models/randomforest_model.pkl       │
│  Method: fsspec + joblib.load()                               │
│  Features: 10 (utan 'return'; inkluderar weekday, vol, m.m.)  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DATA LOADING                                │
│                                                                 │
│  Source: winners-or-loosers.stocks_eu.stock_data              │
│  Query: SELECT ticker, timestamp, open, close, volume          │
│         ORDER BY timestamp DESC LIMIT 1000                    │
│  Location: EU                                                  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FEATURE ENGINEERING                         │
│                                                                 │
│  Same 10 features as training:                                │
│    ├─ weekday, day_of_month                                   │
│    ├─ return_lag1, volatility_5d, volatility_20d              │
│    ├─ momentum_5d, price_above_ma20                           │
│    ├─ ma5_ma20_diff, volume_ratio_20                          │
│    └─ volume_change_1d                                        │
│                                                                 │
│  Data cleaning: dropna()                                      │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PREDICTION GENERATION                       │
│                                                                 │
│  Model: RandomForestClassifier                                 │
│  Target: T+1 (nästa dag)                                       │
│  Input: X (n rader × 10 features)                              │
│  Output:                                                       │
│    ├─ predictions (0 or 1)                                    │
│    └─ probabilities (0.0 to 1.0)                              │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DATA PERSISTENCE                            │
│                                                                 │
│  GCS CSV: gs://polygondata/data/outputs/predictions.csv       │
│  BigQuery: winners-or-loosers.stocks_eu.prediction            │
│                                                                 │
│  Schema:                                                       │
│    ├─ timestamp, ticker                                       │
│    ├─ prediction, probability                                 │
│    ├─ actual_return                                           │
│    └─ load_date                                               │
│                                                                 │
│  Write Mode: WRITE_TRUNCATE (ersätter all data)               │
└─────────────────────────────────────────────────────────────────┘
```

## 📈 MONITORING & ANALYSIS

### 8. Development Analysis: `src/prediction_randomforest.py`
```
┌─────────────────────────────────────────────────────────────────┐
│                DEVELOPMENT & ANALYSIS                          │
│                                                                 │
│  Script: prediction_randomforest.py                            │
│  Purpose: Lokal utveckling och analys                          │
│                                                                 │
│  Features:                                                     │
│    ├─ Samma modell som production                             │
│    ├─ Utökad visualisering (matplotlib, seaborn)              │
│    ├─ Detaljerad evaluation                                   │
│    └─ BigQuery output (WRITE_TRUNCATE)                        │
└─────────────────────────────────────────────────────────────────┘
```

## 🔄 COMPLETE DATA FLOW

```
┌─────────────────────────────────────────────────────────────────┐
│                        START                                   │
│                   (Daglig 06:30/06:35 UTC)                    │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                1. DATA INGESTION                               │
│                                                                 │
│  Airflow DAG → Polygon API → CSV → BigQuery                    │
│  (fetch_and_load_pipeline)                                     │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                2. MODEL TRAINING                               │
│                                                                 │
│  BigQuery → Feature Engineering → Random Forest → GCS          │
│  (train_randomforest.py)                                       │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                3. PREDICTION                                   │
│                                                                 │
│  Airflow DAG → Cloud Run → GCS Model → BigQuery Data →        │
│  Feature Engineering → Predictions → BigQuery Output           │
│  (predict_dag.py → predict.py)                                │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                4. MONITORING                                   │
│                                                                 │
│  BigQuery Tables:                                              │
│    ├─ stock_data (OHLCV data)                                 │
│    └─ prediction (ML predictions)                             │
│                                                                 │
│  GCS Artifacts:                                                │
│    ├─ models/randomforest_model.pkl                           │
│    └─ data/outputs/predictions.csv                            │
└─────────────────────────────────────────────────────────────────┘
```

## 🛠️ TEKNISKA KOMPONENTER

### Airflow DAGs
- **fetch_and_load_pipeline**: Daglig datahämtning från Polygon
- **predict_pipeline**: Daglig ML-prediction via Cloud Run

### Python Scripts
- **fetch_data.py**: Polygon API integration
- **save_to_bigquery.py**: BigQuery data loading
- **train_randomforest.py**: ML model training
- **predict.py**: Production prediction service
- **prediction_randomforest.py**: Development & analysis

### Google Cloud Services
- **BigQuery**: Data warehouse (stocks_eu dataset)
- **Cloud Run**: Serverless ML prediction service
- **Artifact Registry**: Docker image storage
- **Cloud Storage**: Model artifacts & CSV outputs
- **Composer**: Airflow orchestration

### Data Flow Summary
1. **Ingestion**: Polygon API → CSV → BigQuery
2. **Training**: BigQuery → Features → Random Forest → GCS
3. **Prediction**: BigQuery → Features → Model → Predictions → BigQuery
4. **Monitoring**: BigQuery tables för data & predictions

## 🎯 RESULTAT
- **Data**: Daglig OHLCV data från Polygon API
- **ML**: Random Forest modell med 11 features
- **Predictions**: Dagliga förutsägelser om aktieuppgång/nergång
- **Infrastructure**: Fullständigt automatiserat med Airflow + Cloud Run
