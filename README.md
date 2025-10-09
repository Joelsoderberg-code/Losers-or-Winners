# Losers or Winners – Data & ML‑pipeline (GCP)

Kort: daglig ingestion från Polygon → BigQuery → ML‑prediktion i Cloud Run → resultat i BigQuery. Orkestrerat med Airflow (Composer). CI/CD för DAG‑synk och image‑deploy.

## Arkitektur
- Airflow (Composer) – schemaläggning och orkestring
- BigQuery – `stocks_eu.stock_data`, `stocks_eu.prediction`
- GCS – råfiler/artefakter (modell på `gs://polygondata/models/...`)
- Cloud Run Job – `model-scorer` (kör `docker/predict/predict.py`)
- Artifact Registry – Docker‑images

Se även `PROJECT_FLOWCHART.md` (uppdaterat) och Miro‑schemat.

## Pipelines (scheman)
- `fetch_and_load_pipeline`: 16:25 svensk tid (14:25 UTC)
- `predict_pipeline`: 16:30 svensk tid (14:30 UTC)

## Datamodell
- `stocks_eu.stock_data` (BQ): ticker, timestamp, open, close, volume  
  - Laddas via staging + `MERGE` → 1 rad per dag/ticker (idempotent)
- `stocks_eu.prediction` (BQ): timestamp, ticker, prediction, probability, actual_return, load_date  
  - Skrivs med `WRITE_TRUNCATE` vid varje körning

## ML
- Träning: `src/train_randomforest.py` – RandomForest, T+1‑mål, 10 features  
- Produktion: `docker/predict/predict.py` – laddar modell (GCS), läser BQ, skriver BQ

## Konfiguration
- Delbar: `config/config.ini`  
- Hemligheter: `.env` (t.ex. `POLYGON_API_KEY`)  
Läsordning: ENV → `.env` → `config/config.ini` → Airflow Variables (fallback).

## CI/CD
- DAG‑synk (auto): `.github/workflows/sync-dags-to-composer.yml`  
  Synkar `dags/` → `gs://polygondata/dags` via Workload Identity Federation.
- Build & Deploy (manuell eller release): `.github/workflows/build-deploy-model-scorer.yml`  
  Docker build+push → uppdaterar Cloud Run‑jobbet.

 
