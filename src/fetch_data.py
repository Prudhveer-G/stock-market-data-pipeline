"""
fetch_data.py

Fetches historical stock data using yfinance and saves it into the data/ directory.
This forms Step 1 of the Stock Market ETL pipeline.
"""

import os
import yfinance as yf
import pandas as pd

# Configure tickers and period
TICKERS = ["AAPL", "MSFT", "GOOGL"]   # You can add more (TSLA, AMZN, etc.)
PERIOD = "1y"                         # 1 year of historical data

OUTPUT_DIR = "data"


def fetch_ticker(ticker):
    """Download a single ticker as a pandas DataFrame."""
    print(f"Fetching data for: {ticker}")

    df = yf.download(ticker, period=PERIOD, progress=False)
    if df.empty:
        print(f"⚠️ Warning: No data fetched for {ticker}")
        return None

    df = df.reset_index()
    df["ticker"] = ticker  # add ticker as column
    return df


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for ticker in TICKERS:
        df = fetch_ticker(ticker)
        if df is None:
            continue

        save_path = os.path.join(OUTPUT_DIR, f"{ticker}.csv")
        df.to_csv(save_path, index=False)
        print(f"Saved → {save_path}")

    print("\nData fetching complete.")


if __name__ == "__main__":
    main()
