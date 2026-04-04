"""
NYC Mobility friction Data Extractor
Downloads raw NYC Yellow/Green Taxi trips + 311 Service Requests.
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

def download_311(
    start_date: str,
    end_date: str,
    max_records: int = 100_000,
    page_size: int = 5000,
) -> Path | None:
    """Download NYC 311 Service Requests for a given date range.

    Uses the official SODA API with pagination and rate-limit safety.
    Results are saved as a compressed Parquet file.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        max_records: Maximum number of records to fetch (default: 100_000).
        page_size: Records per API page (default: 5000).

    Returns:
        Path to the saved Parquet file, or None if no records were found.

    Raises:
        requests.exceptions.HTTPError: If any API request fails.
    """
    ensure_raw_dirs()

    filename = f"311_{start_date}_{end_date}.parquet"
    out_path = Path("data/raw/311") / filename

    if out_path.exists():
        print(f"311 file already exists: {out_path.name}")
        return out_path

    base_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    offset = 0
    all_records = []
    total = 0

    print(f"⬇️  Fetching 311 data from {start_date} to {end_date}...")

    while True:
        params = {
            "$limit": page_size,
            "$offset": offset,
            "$where": f"created_date >= '{start_date}' AND created_date < '{end_date}'",
            "$order": "created_date ASC",
        }

        resp = requests.get(base_url, params=params, timeout=30)
        resp.raise_for_status()
        page = resp.json()

        if not page:
            break

        all_records.extend(page)
        total += len(page)
        print(f"   Fetched {len(page):,} records → total: {total:,}")

        if len(page) < page_size or total >= max_records:
            break

        offset += page_size
        time.sleep(0.3)  # Be nice to the API

    if not all_records:
        print("No 311 records found for the date range.")
        return None

    df = pd.DataFrame(all_records)
    df.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)

    print(f"Saved {len(df):,} 311 records → {out_path.name}")
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

    parser.add_argument("--311-start", type=str, dest="start_date", help="311 start date YYYY-MM-DD")
    parser.add_argument("--311-end", type=str, dest="end_date", help="311 end date YYYY-MM-DD")
    parser.add_argument(
        "--max-311",
        type=int,
        default=100_000,
        help="Maximum 311 records to fetch (default 100k)",
    )

    args = parser.parse_args()

    if args.taxi_year is not None and args.taxi_month is not None:
        download_taxi(args.taxi_year, args.taxi_month, args.taxi_type)

    if args.start_date and args.end_date:
        download_311(args.start_date, args.end_date, max_records=args.max_311)

    if not any([args.taxi_year, args.start_date]):
        parser.print_help()


if __name__ == "__main__":
    main()
