"""Upload local raw files to the S3-compatible data lake (raw layer)."""
import argparse
import logging
import os
import sys
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
BUCKET = os.getenv("LAKE_BUCKET", "taxi-lake")
NOT_FOUND = {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}

log = logging.getLogger("lake")


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in NOT_FOUND:
            raise
        s3.create_bucket(Bucket=bucket)
        log.info("Created bucket %s", bucket)


def object_key(path: Path) -> str:
    """data/raw/yellow/2025-01/x.parquet  ->  raw/yellow/2025-01/x.parquet"""
    return "raw/" + path.relative_to(DATA_DIR / "raw").as_posix()


def remote_size(s3, bucket: str, key: str):
    """Size of the object in the lake, or None if it does not exist."""
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] in NOT_FOUND:
            return None
        raise


def upload_all(s3, bucket: str, force: bool) -> None:
    files = sorted(
        p for p in (DATA_DIR / "raw").rglob("*")
        if p.is_file() and p.suffix in {".parquet", ".csv"}
    )
    if not files:
        log.warning("No files found under %s", DATA_DIR / "raw")
        return

    uploaded = skipped = 0
    for path in files:
        key = object_key(path)
        size = path.stat().st_size
        if not force and remote_size(s3, bucket, key) == size:
            log.info("Skipping %s (already in lake, same size)", key)
            skipped += 1
            continue
        s3.upload_file(str(path), bucket, key)
        log.info("Uploaded %s (%.1f MB)", key, size / (1024 * 1024))
        uploaded += 1
    log.info("Done: %d uploaded, %d skipped", uploaded, skipped)


def list_objects(s3, bucket: str) -> None:
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            print(f"{obj['Size'] / (1024 * 1024):9.1f} MB  {obj['Key']}")
            count += 1
    print(f"{count} object(s) in s3://{bucket}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="Upload even if the object exists")
    parser.add_argument("--list", action="store_true", help="List objects in the bucket and exit")
    args = parser.parse_args()

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        s3 = get_client()
        ensure_bucket(s3, BUCKET)
        if args.list:
            list_objects(s3, BUCKET)
        else:
            upload_all(s3, BUCKET, args.force)
    except (BotoCoreError, ClientError) as exc:
        log.error("Lake operation failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
