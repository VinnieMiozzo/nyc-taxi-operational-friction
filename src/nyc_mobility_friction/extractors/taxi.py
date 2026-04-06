"""
NYC Mobility friction Data Extractor
Downloads raw NYC Yellow/Green Taxi trips.
"""

from pathlib import Path 
import requests

from .utils import (
        setup_logger,
        ensure_raw_dirs,
        )

from nyc_mobility_friction.paths import get_project_paths

logger = setup_logger(__name__)

def download_taxi_month(
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
    paths = get_project_paths()

    filename = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    out_path = paths.raw / "taxi" / filename

    if out_path.exists():
        logger.info(f"Taxi data already_exists: {out_path.name}")

    logger.info(f"Downloading {taxi_type} taxi data → {filename}")
    response = requests.get(url, stream=True, timeout=60)

    response.raise_for_status()

    with open(out_path, "wb") as f:
        for chunk in response.iter_content(chunk_size = 8*1024):
            f.write(chunk)
    
    logger.info(f"Saved {out_path.name} ({out_path.stat().st_size / 1_000_000:.1f} MB)")
    return out_path
