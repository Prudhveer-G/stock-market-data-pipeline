# train_model.py (also used as load)
import os
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import logging
from datetime import datetime

logger = logging.getLogger("train_model")
OUTPUT_DIR = "output"

def write_local_parquet(df, ticker):
    date_str = df["date"].dt.date.iloc[0].isoformat()
    out_dir = os.path.join(OUTPUT_DIR, ticker, f"date={date_str}")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"part-{datetime.utcnow().strftime('%H%M%S')}.parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, out_path)
    logger.info("Wrote local parquet: %s", out_path)
    return out_path

def upload_parquet(local_path, ticker, bucket=None):
    bucket = bucket or os.environ.get("S3_BUCKET")
    if not bucket:
        raise ValueError("S3_BUCKET not set")
    date_part = os.path.basename(os.path.dirname(local_path))
    key = f"{ticker}/{date_part}/{os.path.basename(local_path)}"
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)
    logger.info("Uploaded to s3: s3://%s/%s", bucket, key)
    return f"s3://{bucket}/{key}"
