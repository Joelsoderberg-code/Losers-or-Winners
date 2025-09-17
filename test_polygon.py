import os

import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("POLYGON_API_KEY")

url = f"https://api.polygon.io/v2/aggs/ticker/AAPL/prev?apiKey={api_key}"
r = requests.get(url)

print("Status:", r.status_code)
print("Response:", r.text)
