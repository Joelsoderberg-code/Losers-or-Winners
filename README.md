# Losers or Winners

Proof-of-Concept för tradingstrategi med risk/reward-analys och maskininlärning.

## Sprint Week 1
- Sätta upp repo
- Skapa Jira & Miro för sprintöverblick
- Bestämma API för marknadsdata
- Bygga baseline-modell

### API-alternativ
- Alpha Vantage
- Finnhub
- Marketstack
- Riksbanken / SCB (svenska data)

---

## Project Setup (English)

This repository contains the initial setup for the **Losers or Winners** project.
The goal is to build a financial trading strategy using historical and updated market data, analyzed with machine learning models, to predict whether a strategy is a **winner or loser**.

### Environment Setup
We included an `example.env` file.
Each team member should copy this file into their personal folder and rename it to `.env`, then add their own credentials:

```bash
# Example environment variables – replace with your own values:
GOOGLE_APPLICATION_CREDENTIALS=your-service-account-key.json
PROJECT_ID=winners-or-loosers
BQ_DATASET_ID=raw_data
BQ_TABLE_ID=winners_or_loosers

Security

A .gitignore file has been configured to make sure sensitive files (like personal keys) are never committed to GitHub.
However, always double-check with git status before pushing, as manually staged files can still be committed.

Workflow

The shared repository contains the base project setup.

Each team member should place their personal service account key in their own environment, not in this repository.

Contributions should be made via feature branches and merged with pull requests.

feature/airflow-pipeline
---

## Airflow – installation och körning (lokalt)

1) Skapa och aktivera venv
- Varför: Isolerar projektets Python-paket så de inte krockar med systemet/andra projekt.
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2) Installera projektberoenden (utan Airflow)
- Varför: Lägger in bibliotek som BigQuery-klienten, pandas, osv. som koden använder.
```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

3) Installera Airflow med constraints (viktigt för Python 3.12)
- Varför: Airflow har många beroenden; constraints-filen garanterar kompatibla versioner.
```bash
pip install "apache-airflow==2.9.3" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt
```

4) Miljövariabler och sökvägar
- Varför: `PYTHONPATH` gör att Airflow hittar din kod under projektroten. `DAGS_FOLDER` pekar Airflow till dina DAG-filer.
```bash
export PYTHONPATH=$(pwd)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
```

5) Initiera och starta Airflow
- Varför: `db init` skapar Airflows interna databas. Användaren behövs för att logga in i UI. Webservern visar UI på port 8080 och scheduler kör dina jobb.
```bash
airflow db init
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin --email admin@example.com
airflow webserver --port 8080 &
airflow scheduler &
```

6) Sätt Airflow Variables (ersätt med riktiga värden)
- Varför: Centralt ställe för konfiguration (API-nycklar, datumintervall, filvägar) som koden läser vid körning.
```bash
airflow variables set POLYGON_API_KEY DIN_NYCKEL
airflow variables set TICKER AAPL
airflow variables set START_DATE 2025-09-01
airflow variables set END_DATE 2025-09-03
airflow variables set OUTPUT_DIR $(pwd)/data
airflow variables set OUTPUT_FILE stock_data.csv
airflow variables set CSV_PATH $(pwd)/data/stock_data.csv
airflow variables set BQ_DATASET stocks
airflow variables set BQ_TABLE stock_data
airflow variables set BQ_WRITE_DISPOSITION WRITE_APPEND
# valfritt, om servicekonto används för GCP (krävs för server/CI)
airflow variables set GOOGLE_APPLICATION_CREDENTIALS /absolut/sökväg/service_account.json
```

7) Kör DAG:en
- Varför: Startar ett körningstillfälle av din pipeline (kan också schemaläggas automatiskt).
```bash
airflow dags trigger fetch_and_load_pipeline
```

Tips:
- Om `airflow`-kommandot inte hittas, kör det via venv: `$(pwd)/.venv/bin/airflow`.
- Kör inte `dags/*.py` direkt med `python`; Airflow laddar dem automatiskt.
=======
### Getting Started

1. Clone the repository
2. Create your own `.env` file based on `example.env`
3. Add your personal service account key to the project root
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
5. - Run the project: python3 main.py
   Make sure you have access to the required GCP resources (BigQuery, Storage).
 main
