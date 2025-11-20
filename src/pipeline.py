#!/usr/bin/env python3
"""
pipeline.py — orchestrator (fetch -> process -> train/load)
Run locally: python src/pipeline.py --ticker TEST --mode local
"""
import argparse
import logging
from fetch_data import fetch_data      # existing file
from process_data import normalize_df  # existing file
from train_model import write_local_parquet, upload_parquet  # reuse train_model.py

# simple plain-text logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("pipeline")

def run(ticker: str, mode: str = "local"):
    logger.info("Starting pipeline", extra={"ticker": ticker, "mode": mode})
    raw = fetch_data(ticker)
    df = normalize_df(raw)
    out_path = write_local_parquet(df, ticker)
    if mode == "s3":
        upload_parquet(out_path, ticker)
    logger.info("Pipeline finished", extra={"output": out_path})

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", required=True)
    p.add_argument("--mode", choices=["local","s3"], default="local")
    args = p.parse_args()
    run(args.ticker, args.mode)
