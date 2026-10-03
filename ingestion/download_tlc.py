"""Download one month of NYC yellow taxi trip data into the raw layer."""
import argparse
import logging
import os
import re
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()  # reads .env so DATA_DIR and LOG_LEVEL can be configured

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

log = logging.getLogger("ingestion")


def build_url(month: str) -> str:
    return f"{BASE_URL}/yellow_tripdata_{month}.parquet"


def target_path(month: str) -> Path:
    return DATA_DIR / "raw" / "yellow" / month / f"yellow_tripdata_{month}.parquet"


def download_month(month: str) -> Path:
    dest = target_path(month)

    if dest.exists():
        log.info("Skipping %s: file already exists", dest)
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    url = build_url(month)
    log.info("Downloading %s", url)

    try:
        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            with open(tmp, "wb") as out:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    out.write(chunk)
        tmp.rename(dest)
    except Exception:
        tmp.unlink(missing_ok=True)  # never leave a half-downloaded file
        raise

    size_mb = dest.stat().st_size / (1024 * 1024)
    log.info("Saved %s (%.1f MB)", dest, size_mb)
    return dest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="Month as YYYY-MM, e.g. 2025-01")
    args = parser.parse_args()

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.month):
        log.error("Invalid month %r, expected YYYY-MM", args.month)
        return 2

    try:
        download_month(args.month)
    except requests.RequestException as exc:
        log.error("Download failed for %s: %s", args.month, exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
