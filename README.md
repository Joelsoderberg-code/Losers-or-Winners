# Winners or Loosers

Questo progetto è una pipeline di dati in Google Cloud BigQuery per analizzare strategie finanziarie 
e determinare se una strategia è **Winner** o **Looser**.

## 📂 Struttura della cartella

```
winners_or_loosers/
├── Dockerfile
├── docker-compose.yml
├── main.py
├── requirements.txt
├── example.env
├── .gitignore
└── README.md
```

## ⚙️ Configurazione

1. Clonare la repository da GitHub:
   ```bash
   git clone <repo-url>
   cd winners_or_loosers
   ```

2. Creare un file `.env` partendo da `example.env`:
   ```bash
   cp example.env .env
   ```

3. Aprire `.env` e inserire i dati reali del progetto:

   ```env
   PROJECT_ID=winners-or-loosers
   BQ_DATASET_ID=raw_data
   BQ_TABLE_ID=beginning
   ```

4. Aggiungere **la propria chiave privata JSON** (`service-account-key.json`) nella cartella, ma **NON condividerla né caricarla su GitHub**.

## 🚀 Esecuzione

Con Docker:
```bash
docker build -t winners_or_loosers .
docker run --env-file .env winners_or_loosers
```

Con Docker Compose:
```bash
docker-compose up --build
```

## 🔑 Sicurezza

- `service-account-key.json` deve rimanere solo in locale.
- Il file `.gitignore` è già configurato per ignorare `.env` e le chiavi JSON.
- Ogni membro del team deve avere la propria chiave e creare il proprio `.env`.

## 👥 Collaborazione

- Il `PROJECT_ID` è condiviso da tutti.
- Ogni sviluppatore deve inserire la propria chiave nella cartella locale.
- Su GitHub vengono salvati solo i file comuni del progetto, mai le chiavi.

---
📌 Autore: Team Winners or Loosers
