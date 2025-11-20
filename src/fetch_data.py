# fetch_data.py (ingest)
import logging
from tenacity import retry, wait_exponential, stop_after_attempt

logger = logging.getLogger("fetch_data")
BASE_URL = "https://example.com/api"  # replace when you add real API

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def fetch_data(ticker: str):
    """
    Fetch raw market data for ticker.
    For now this returns a local stub for quick testing.
    Replace with actual requests.get(...) when ready.
    """
    logger.info("fetch_data: returning local stub for %s", ticker)
    return {
        "prices": [
            {
                "date": "2025-01-01T09:15:00+05:30",
                "open": "100",
                "high": "110",
                "low": "95",
                "close": "105",
                "volume": "1000"
            }
        ]
    }
