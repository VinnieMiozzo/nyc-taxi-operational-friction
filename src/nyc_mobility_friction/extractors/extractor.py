"""
NYC Mobility friction Data Extractor
Downloads raw NYC Yellow/Green Taxi trips.
"""

from pathlib import Path
import argparse
import time 
import requests
import pandas as pd


def ensure_raw_dirs() -> None:
    """Create data/raw/taxi/ and data/raw/311/ folders.

    creates:
        data/raw/taxi/
        data/raw/311/
    """
    base = Path("data/raw")
    base.mkdir(parents = True, exist_ok = True)
    (base / "taxi").mkdir(exist_ok = True)
    (base / "311").mkdir(exist_ok = True)

def download_taxi(
        year: int, month: int, taxi_type: str = "yellow"
        ) -> Path:
    """Download one month of NYC TLC taxi trip data in Parquet format.

    The data is fetched from the official NYC TLC CloudFront endpoint.

    Args:
        year: Four-digit year (e.g. 2025).
        month: Month number (1-12).
        taxi_type: Either "yellow" or "green" (default: "yellow")

    Returns:
        Path to the downloaded Parquet file

    Raises:
        requests.exceptions.HTTPError: If download fails.
    """
    ensure_raw_dirs()

    filename = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    out_path = Path("data/raw/taxi") / filename

    if out_path.exists():
        print(f"Taxi data already_exists: {out_path.name}")

    print(f"Downloading {taxi_type} taxi data → {filename}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    with open(out_path, "wb") as f:
        for chunk in response.iter_content(chunk_size = 8*1024):
            f.write(chunk)
    
    print(f"Saved {out_path.name} ({out_path.stat().st_size / 1_000_000:.1f} MB)")
    return out_path

def main() -> None:
    """Parse command-line arguments and trigger the appropriate downloads.

    Supports downloading taxi data, 311 data, or both in a single run.
    """
    parser = argparse.ArgumentParser(
        description="Extract NYC Taxi + 311 data for your mobility project"
    )
    parser.add_argument("--taxi-year", type=int, help="Taxi year (e.g. 2025)")
    parser.add_argument("--taxi-month", type=int, help="Taxi month (1-12)")
    parser.add_argument(
        "--taxi-type",
        choices=["yellow", "green"],
        default="yellow",
        help="Taxi type (default: yellow)",
    )

    args = parser.parse_args()

    if args.taxi_year is not None and args.taxi_month is not None:
        download_taxi(args.taxi_year, args.taxi_month, args.taxi_type)

    if not any(args.taxi_year):
        parser.print_help()


if __name__ == "__main__":
    main()
