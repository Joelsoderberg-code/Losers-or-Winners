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
GOOGLE_APPLICATION_CREDENTIALS=your-service-account-key.json
PROJECT_ID=winners-or-loosers
BQ_DATASET_ID=raw_data
BQ_TABLE_ID=winners_or_loosers

Security

A .gitignore file has been configured to make sure sensitive files (like personal keys) are never committed to GitHub.
Even if a personal key is added by mistake, it will not be pushed.

Workflow

The shared repository contains the base project setup.

Each team member should place their personal service account key in their own environment, not in this repository.

Contributions should be made via feature branches and merged with pull requests.