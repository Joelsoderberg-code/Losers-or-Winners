"""Check which dates already exist in BigQuery to avoid duplicates."""

from datetime import datetime, timedelta

from google.cloud import bigquery


def get_existing_dates(table_id: str, ticker: str = "SPY") -> set:
    """Get all existing dates for a ticker from BigQuery.

    Args:
        table_id: BigQuery table ID (e.g., "project.dataset.table")
        ticker: Ticker symbol to check

    Returns:
        Set of existing dates as strings (YYYY-MM-DD format)
    """
    client = bigquery.Client()

    query = f"""
    SELECT DISTINCT DATE(timestamp) as date
    FROM `{table_id}`
    WHERE ticker = @ticker
    ORDER BY date
    """

    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)])

    try:
        results = client.query(query, job_config=job_config, location="EU")
        existing_dates = {row.date.isoformat() for row in results}
        print(f"Found {len(existing_dates)} existing dates for {ticker}")
        return existing_dates
    except Exception as e:
        print(f"Error checking existing dates: {e}")
        return set()


def get_missing_dates(start_date: str, end_date: str, existing_dates: set) -> list:
    """Get list of missing dates between start_date and end_date.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        existing_dates: Set of existing dates

    Returns:
        List of missing dates as strings
    """
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()

    missing_dates = []
    current = start

    while current <= end:
        date_str = current.isoformat()
        if date_str not in existing_dates:
            missing_dates.append(date_str)
        current += timedelta(days=1)

    print(f"Missing dates between {start_date} and {end_date}: {len(missing_dates)}")
    return missing_dates


if __name__ == "__main__":
    # Test the functions
    table_id = "winners-or-loosers.stocks_eu.stock_data"
    existing = get_existing_dates(table_id)
    print(f"Existing dates: {sorted(list(existing))[-10:]}")  # Show last 10 dates
