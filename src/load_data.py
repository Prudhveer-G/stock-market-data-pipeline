import logging
import mysql.connector
import pandas as pd

logger = logging.getLogger("load_data")

def load_to_mysql(df: pd.DataFrame, connection):
    """
    Load transformed stock data into MySQL.
    Expects dataframe with columns:
    date, open, high, low, close, volume, ticker
    """
    insert_query = """
        INSERT INTO stock_prices
        (ticker, trade_date, open_price, high_price, low_price, close_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open_price = VALUES(open_price),
            high_price = VALUES(high_price),
            low_price = VALUES(low_price),
            close_price = VALUES(close_price),
            volume = VALUES(volume);
    """

    records = [
        (
            row.ticker,
            row.date.date(),
            row.open,
            row.high,
            row.low,
            row.close,
            row.volume
        )
        for row in df.itertuples(index=False)
    ]

    cursor = connection.cursor()
    cursor.executemany(insert_query, records)
    connection.commit()

    logger.info("Loaded %d records into MySQL", len(records))
