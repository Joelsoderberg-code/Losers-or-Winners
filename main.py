import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID")

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Example query to test the connection
query = f"""
SELECT *
FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`
LIMIT 5
"""

print("Running query on BigQuery...")
query_job = client.query(query)
results = query_job.result()

# Print results as dictionaries
for row in results:
    print(dict(row))
