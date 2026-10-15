import glob
import os

import boto3
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_RAW = os.environ.get("MINIO_BUCKET_RAW", "raw")

LOCAL_RAW_DIR = os.path.join(os.path.dirname(__file__), "_local_raw")


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket: str):
    existing = [b["Name"] for b in client.list_buckets()["Buckets"]]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)


def upload_pending_files():
    client = get_client()
    ensure_bucket(client, BUCKET_RAW)

    files = sorted(glob.glob(os.path.join(LOCAL_RAW_DIR, "orders_*.parquet")))
    for filepath in files:
        key = f"orders/{os.path.basename(filepath)}"
        client.upload_file(filepath, BUCKET_RAW, key)
        print(f"[staging] Uploaded {filepath} -> s3://{BUCKET_RAW}/{key}")


if __name__ == "__main__":
    upload_pending_files()

