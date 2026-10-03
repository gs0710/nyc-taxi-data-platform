"""Ingest NYC TLC data (yellow trips by month, zone lookup) into the raw layer."""
import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
TRIPS_URL = os.getenv("TLC_TRIPS_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data")
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
MAX_ATTEMPTS = 4
METADATA_FILE = DATA_DIR / "raw" / "_metadata" / "ingestion_log.jsonl"

log = logging.getLogger("ingestion")


class ValidationError(Exception):
    """The downloaded file is not what we expected."""


def stream_to_file(url: str, tmp: Path):
    """Download url to tmp in chunks. Returns (size_bytes, sha256_hex)."""
    sha = hashlib.sha256()
    size = 0
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(tmp, "wb") as out:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                out.write(chunk)
                sha.update(chunk)
                size += len(chunk)
    return size, sha.hexdigest()


def fetch_with_retries(url: str, tmp: Path):
    """Retry temporary failures with exponential backoff; fail fast on permanent ones."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return stream_to_file(url, tmp)
        except requests.HTTPError as exc:
            status = exc.response.status_code
            if status < 500 and status != 429:
                raise  # 403/404 etc. will not fix themselves, so do not retry
            error = exc
        except (requests.ConnectionError, requests.Timeout) as exc:
            error = exc

        if attempt == MAX_ATTEMPTS:
            raise error
        wait = 2 ** attempt
        log.warning("Attempt %d/%d failed (%s). Retrying in %ds",
                    attempt, MAX_ATTEMPTS, error, wait)
        time.sleep(wait)


def validate_parquet(path: Path) -> int:
    """Read only the Parquet footer. Returns the row count."""
    try:
        return pq.ParquetFile(path).metadata.num_rows
    except Exception as exc:
        raise ValidationError(f"Not a readable Parquet file: {exc}") from exc


def validate_zones(path: Path) -> int:
    """Check the zone CSV header and that it has a plausible number of rows."""
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or "LocationID" not in lines[0]:
        raise ValidationError("Zone file header does not contain LocationID")
    rows = len(lines) - 1
    if rows < 200:
        raise ValidationError(f"Zone file has only {rows} rows, expected about 265")
    return rows


def write_metadata(record: dict) -> None:
    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(METADATA_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def ingest(url: str, dest: Path, validate, dataset: str, force: bool) -> None:
    if dest.exists() and not force:
        log.info("Skipping %s: file already exists (use --force to replace)", dest)
        return

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    started = time.time()
    log.info("Downloading %s", url)

    try:
        size, sha256 = fetch_with_retries(url, tmp)
        rows = validate(tmp)
        tmp.replace(dest)  # atomic: the final name only ever points at a validated file
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    write_metadata({
        "dataset": dataset,
        "source_url": url,
        "file": str(dest),
        "bytes": size,
        "sha256": sha256,
        "rows": rows,
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_seconds": round(time.time() - started, 1),
    })
    log.info("Saved %s (%.1f MB, %d rows, sha256 %s...)",
             dest, size / (1024 * 1024), rows, sha256[:12])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", help="Yellow trips month as YYYY-MM, e.g. 2025-01")
    parser.add_argument("--zones", action="store_true", help="Download the taxi zone lookup")
    parser.add_argument("--force", action="store_true", help="Re-download even if the file exists")
    args = parser.parse_args()

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.month and not args.zones:
        parser.error("give --month YYYY-MM and/or --zones")
    if args.month and not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.month):
        log.error("Invalid month %r, expected YYYY-MM", args.month)
        return 2

    try:
        if args.zones:
            ingest(ZONES_URL, DATA_DIR / "raw" / "zones" / "taxi_zone_lookup.csv",
                   validate_zones, "taxi_zone_lookup", args.force)
        if args.month:
            ingest(f"{TRIPS_URL}/yellow_tripdata_{args.month}.parquet",
                   DATA_DIR / "raw" / "yellow" / args.month / f"yellow_tripdata_{args.month}.parquet",
                   validate_parquet, f"yellow_trips_{args.month}", args.force)
    except requests.RequestException as exc:
        log.error("Download failed: %s", exc)
        return 1
    except ValidationError as exc:
        log.error("Validation failed: %s", exc)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
