#!/usr/bin/env python3
"""
pipeline.py — orchestrator (fetch -> process -> load)
Run locally: python src/pipeline.py --ticker TEST
"""
import argparse
import logging
import mysql.connector

from fetch_data import fetch_data
from process_data import normalize_df
from load_data import load_to_mysql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("pipeline")


def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="stocks"
    )


def run(ticker: str):
    logger.info("Starting pipeline for %s", ticker)

    raw = fetch_data(ticker)
    df = normalize_df(raw)

    df["ticker"] = ticker

    conn = get_mysql_connection()
    load_to_mysql(df, conn)
    conn.close()

    logger.info("Pipeline finished for %s", ticker)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", required=True)
    args = parser.parse_args()

    run(args.ticker)
