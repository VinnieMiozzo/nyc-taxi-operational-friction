"""
NYC Mobility Friction Data Extractor
Downloads daily historical weather for NYC using Open-Meteo archive API.
"""

from pathlib import Path
import requests
import pandas as pd

from .utils import (
    setup_logger,
    ensure_external_dirs,
)

from nyc_mobility_friction.paths import get_project_paths

logger = setup_logger(__name__)


def extract_weather(
    start_date: str = "2025-01-01",
    end_date: str = "2025-03-31"
) -> Path:
    """Download daily weather data for NYC from Open-Meteo archive API.

    The data is fetched from the public Open-Meteo historical archive.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.

    Returns:
        Path to the saved CSV file.

    Raises:
        requests.exceptions.HTTPError: If the API request fails.
    """
    ensure_external_dirs()
    paths = get_project_paths()

    filename = f"nyc_daily_weather_{start_date[:7]}_{end_date[:7]}.csv"
    out_path = paths.raw / "external" / filename

    if out_path.exists():
        logger.info(f"Weather data already exists: {out_path.name}")
        return out_path

    logger.info(f"Downloading weather data → {start_date} to {end_date}")

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.78,
        "longitude": -73.97,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,precipitation_hours,wind_speed_10m_max",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": "America/New_York"
    }

    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()

        data = r.json()["daily"]
        weather = pd.DataFrame(data)

        # Clean and enrich
        weather["date"] = pd.to_datetime(weather["time"]).dt.date
        weather = weather.drop(columns=["time"])
        weather["temp_avg_f"] = (weather["temperature_2m_max"] + weather["temperature_2m_min"]) / 2
        weather["has_precip"] = weather["precipitation_sum"] > 0
        weather["has_snow"] = weather["snowfall_sum"] > 0

        weather.to_csv(out_path, index=False)

        logger.info(f"Saved {out_path.name} ({len(weather)} days)")
        return out_path

    except Exception as e:
        logger.error(f"Failed to download weather data: {e}")
        raise
