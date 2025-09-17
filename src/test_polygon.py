import os

from polygon import RESTClient

api_key = os.getenv("POLYGON_API_KEY")
client = RESTClient(api_key)

# Hämta AAPL dagliga prisdata för januari 2023
aggs = client.get_aggs("AAPL", 1, "day", "2025-08-01", "2025-08-29")

for agg in aggs:
    print(f"Date: {agg.timestamp}, Open: {agg.open}, Close: {agg.close}, Volume: {agg.volume}")
