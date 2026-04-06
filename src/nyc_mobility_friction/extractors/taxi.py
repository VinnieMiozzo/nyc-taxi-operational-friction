"""
NYC Mobility friction Data Extractor
Downloads raw NYC Yellow/Green Taxi trips.
"""

from pathlib import Path 
import requests

from .utils import (
        ensure_raw_dirs,
        )

from nyc_mobility_friction.paths import get_project_paths

import logging
logger = logging.getLogger(__name__)

def download_taxi_month(
        year: int, month: int, taxi_type: str = "yellow", force: bool = False
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

    if out_path.exists() and not force:
        logger.info(f"Using existing file: {out_path.name}")
        return out_path

    logger.info(f"Downloading {taxi_type} taxi data → {filename}")
    response = requests.get(url, stream=True, timeout=60)

    response.raise_for_status()

    temp_path = out_path.with_suffix(out_path.suffix + ".part")

    with open(temp_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    
    temp_path.replace(out_path)
    logger.info(f"Saved {out_path.name} ({out_path.stat().st_size / 1_000_000:.1f} MB)")
    return out_path
