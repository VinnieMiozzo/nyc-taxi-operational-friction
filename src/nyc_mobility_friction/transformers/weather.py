"""
Weather transformer for the NYC Mobility Friction project.

This module converts raw daily weather data into a cleaned daily dataset
for downstream joins with taxi zone-day data.
"""

from pathlib import Path
import logging

import pandas as pd

from nyc_mobility_friction.paths import get_project_paths

logger = logging.getLogger(__name__)


def transform_weather(
    start_date: str,
    end_date: str,
    force: bool = False,
) -> Path:
    """Transform raw daily weather data for a requested date range.

    Args:
        start_date: Inclusive start date in YYYY-MM-DD format.
        end_date: Inclusive end date in YYYY-MM-DD format.
        force: Overwrite an existing processed file if True.

    Returns:
        Path to the saved processed weather CSV file.
    """
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    if start_ts > end_ts:
        raise ValueError(f"start_date ({start_date}) must be <= end_date ({end_date})")

    paths = get_project_paths()

    in_path = paths.raw / "external" / f"nyc_daily_weather_{start_date}_{end_date}.csv"
    if not in_path.exists():
        raise FileNotFoundError(f"Missing raw weather file: {in_path.name}")

    out_dir = paths.processed / "weather"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"weather_daily_{start_date}_{end_date}.csv"
    temp_path = out_path.with_suffix(".part.csv")

    if out_path.exists() and not force:
        logger.info(f"Using existing processed weather file: {out_path.name}")
        return out_path

    logger.info(f"Transforming weather data for {start_date} to {end_date}")

    df = pd.read_csv(in_path)
    df["date"] = pd.to_datetime(df["date"])

    df = df.loc[
        (df["date"] >= start_ts) & (df["date"] <= end_ts),
        :
    ].copy()

    keep_cols = [
        "date",
        "weather_code",
        "temperature_2m_max",
        "temperature_2m_min",
        "temp_avg_f",
        "precipitation_sum",
        "snowfall_sum",
        "precipitation_hours",
        "wind_speed_10m_max",
        "has_precip",
        "has_snow",
    ]
    existing_cols = [col for col in keep_cols if col in df.columns]
    df = df.loc[:, existing_cols].sort_values("date").reset_index(drop=True)

    df.to_csv(temp_path, index=False)
    temp_path.replace(out_path)

    logger.info(f"Saved processed weather file: {out_path.name} ({len(df):,} rows)")
    return out_path
