"""
Calendar transformer for the NYC Mobility Friction project.

This module converts raw holiday data into a cleaned daily calendar
dataset for downstream joins with taxi zone-day data.
"""

from pathlib import Path
import logging

import pandas as pd

from nyc_mobility_friction.paths import get_project_paths

logger = logging.getLogger(__name__)


def transform_calendar(
    years: list[int],
    force: bool = False,
) -> Path:
    """Transform raw holiday data for the requested years.

    Args:
        years: List of four-digit years.
        force: Overwrite an existing processed file if True.

    Returns:
        Path to the saved processed calendar CSV file.
    """
    if not years:
        raise ValueError("years must not be empty")

    years = sorted(set(years))
    paths = get_project_paths()

    in_path = paths.raw / "external" / f"holidays_{years[0]}_{years[-1]}.csv"
    if not in_path.exists():
        raise FileNotFoundError(f"Missing raw holiday file: {in_path.name}")

    out_dir = paths.processed / "calendar"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"calendar_daily_{years[0]}_{years[-1]}.csv"
    temp_path = out_path.with_suffix(".part.csv")

    if out_path.exists() and not force:
        logger.info(f"Using existing processed calendar file: {out_path.name}")
        return out_path

    logger.info(f"Transforming holiday data for years: {years}")

    holidays_df = pd.read_csv(in_path)
    holidays_df["date"] = pd.to_datetime(holidays_df["date"])

    calendar_df = pd.DataFrame(
        {"date": pd.date_range(f"{years[0]}-01-01", f"{years[-1]}-12-31", freq="D")}
    )

    calendar_df = calendar_df.merge(holidays_df, on="date", how="left")
    calendar_df["is_holiday"] = calendar_df["holiday_name"].notna()
    calendar_df["day_of_week"] = calendar_df["date"].dt.day_name()
    calendar_df["day_of_week_num"] = calendar_df["date"].dt.dayofweek
    calendar_df["is_weekend"] = calendar_df["day_of_week_num"].isin([5, 6])

    calendar_df.to_csv(temp_path, index=False)
    temp_path.replace(out_path)

    logger.info(f"Saved processed calendar file: {out_path.name} ({len(calendar_df):,} rows)")
    return out_path
