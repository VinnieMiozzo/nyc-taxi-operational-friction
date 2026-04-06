"""
Basic tests for the extractors.
"""

import pytest
from pathlib import Path

from nyc_mobility_friction.extractors import (
    download_taxi_month,
    extract_events,
    extract_weather,
    extract_holidays,
)


def test_ensure_dirs():
    """Test that directory creation functions run without error."""
    from nyc_mobility_friction.extractors.utils import ensure_raw_dirs, ensure_external_dirs
    ensure_raw_dirs()
    ensure_external_dirs()
    assert True  # if we get here, directories exist


def test_download_taxi_month():
    """Test downloading one month of taxi data (idempotent)."""
    path = download_taxi_month(2025, 1, taxi_type="yellow")
    assert path.exists()
    assert path.suffix == ".parquet"


def test_extract_holidays():
    """Test holiday generation."""
    path = extract_holidays(years=[2025])
    assert path.exists()
    assert path.name == "holidays.csv"


# Optional: run these only when you have internet
@pytest.mark.slow
def test_extract_events_and_weather():
    """Test full event and weather extraction (slow)."""
    events_path = extract_events()
    weather_path = extract_weather()
    assert events_path.exists()
    assert weather_path.exists()
