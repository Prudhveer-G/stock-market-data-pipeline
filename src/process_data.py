# process_data.py (transform)
import pandas as pd
import logging

logger = logging.getLogger("process_data")

def normalize_df(raw_json):
    """
    Convert raw JSON to dataframe with exact columns:
    date (UTC), open, high, low, close, volume
    """
    prices = raw_json.get("prices") or raw_json
    df = pd.DataFrame(prices)
    df = df.rename(columns=lambda c: c.strip().lower())

    if "date" not in df.columns:
        raise ValueError("Missing 'date' in raw data")

    df["date"] = pd.to_datetime(df["date"], utc=True)

    needed = ["date","open","high","low","close","volume"]
    for col in needed:
        if col not in df.columns:
            df[col] = None

    df = df[needed]
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].apply(pd.to_numeric, errors="coerce")

    logger.info("normalize_df: rows=%d", len(df))
    return df
