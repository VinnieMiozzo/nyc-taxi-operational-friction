"""
Weather extractor for the NYC Mobility Friction project.

This module downloads daily historical NYC weather from the Open-Meteo
archive API for a requested date range and saves the raw output under
data/raw/external/.
"""


from pathlib import Path
import logging

import pandas as pd

from .utils import ensure_external_dirs, make_session
from nyc_mobility_friction.paths import get_project_paths

logger = logging.getLogger(__name__)


def extract_weather(
    start_date: str = "2025-01-01",
    end_date: str = "2025-03-31",
    force: bool = False,
) -> Path:
    """Download daily historical NYC weather for a requested date range.

    Args:
        start_date: Inclusive start date in YYYY-MM-DD format.
        end_date: Inclusive end date in YYYY-MM-DD format.
        force: Overwrite an existing file if True.

    Returns:
        Path to the saved CSV file.
    """
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    if start_ts > end_ts:
        raise ValueError(f"start_date ({start_date}) must be <= end_date ({end_date})")

    ensure_external_dirs()
    paths = get_project_paths()

    filename = f"nyc_daily_weather_{start_date}_{end_date}.csv"
    out_path = paths.raw / "external" / filename
    temp_path = out_path.with_suffix(".part.csv")

    if out_path.exists() and not force:
        logger.info(f"Weather data already exists: {out_path.name}")
        return out_path

    logger.info(f"Downloading weather data -> {start_date} to {end_date}")

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.78,
        "longitude": -73.97,
        "start_date": start_date,
        "end_date": end_date,
        "daily": (
            "weather_code,"
            "temperature_2m_max,"
            "temperature_2m_min,"
            "precipitation_sum,"
            "snowfall_sum,"
            "precipitation_hours,"
            "wind_speed_10m_max"
        ),
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": "America/New_York",
    }

    session = make_session()

    try:
        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()

        payload = response.json()
        if "daily" not in payload:
            raise ValueError("Weather API response missing 'daily' field.")

        weather = pd.DataFrame(payload["daily"])
        if "time" not in weather.columns:
            raise ValueError("Weather API response missing 'time' column.")

        weather["date"] = pd.to_datetime(weather["time"]).dt.date
        weather = weather.drop(columns=["time"])
        weather["temp_avg_f"] = (
            weather["temperature_2m_max"] + weather["temperature_2m_min"]
        ) / 2
        weather["has_precip"] = weather["precipitation_sum"] > 0
        weather["has_snow"] = weather["snowfall_sum"] > 0

        weather.to_csv(temp_path, index=False)
        temp_path.replace(out_path)

        logger.info(f"Saved {out_path.name} ({len(weather)} days)")
        return out_path

    except Exception:
        temp_path.unlink(missing_ok=True)
        logger.exception(f"Failed to download weather data for {start_date} to {end_date}")
        raise
