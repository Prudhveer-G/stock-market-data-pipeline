import pandas as pd
import os

INPUT_PATH = "data/raw_stock_data.csv"
OUTPUT_PATH = "data/processed_stock_data.csv"

def load_data(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found. Run fetch_data.py first.")
    return pd.read_csv(path)

def clean_data(df):
    # Remove duplicate rows
    df = df.drop_duplicates()

    # Forward-fill missing values
    df = df.fillna(method="ffill")

    # Drop any remaining missing
    df = df.dropna()

    return df

def engineer_features(df):
    # Moving averages (common financial features)
    df["MA_5"] = df["Close"].rolling(5).mean()
    df["MA_20"] = df["Close"].rolling(20).mean()

    # Daily returns
    df["Daily_Return"] = df["Close"].pct_change()

    # Volatility (past 10 days)
    df["Volatility_10"] = df["Daily_Return"].rolling(10).std()

    # Drop rows that became NaN from rolling windows
    df = df.dropna()

    return df

def main():
    print("Loading raw stock data...")
    df = load_data(INPUT_PATH)

    print("Cleaning data...")
    df = clean_data(df)

    print("Engineering features...")
    df = engineer_features(df)

    print(f"Saving processed data to {OUTPUT_PATH}")
    df.to_csv(OUTPUT_PATH, index=False)

    print("Done.")

if __name__ == "__main__":
    main()
