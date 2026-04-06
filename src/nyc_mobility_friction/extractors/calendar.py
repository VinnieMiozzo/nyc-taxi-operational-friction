"""
NYC Mobility Friction Data Extractor
Generates US holidays (including observed dates) for given years.
"""

from pathlib import Path
import pandas as pd
import holidays

from .utils import (
    setup_logger,
    ensure_external_dirs,
)

from nyc_mobility_friction.paths import get_project_paths

logger = setup_logger(__name__)


def extract_holidays(years: list[int]) -> Path:
    """Generate US holidays CSV for the requested years.

    The holidays include observed dates (e.g. when a holiday falls on a weekend).

    Args:
        years: List of four-digit years (e.g. [2024, 2025]).

    Returns:
        Path to the saved holidays.csv file.
    """
    ensure_external_dirs()
    paths = get_project_paths()

    filename = f"holidays_{years[0]}.csv"
    out_path = paths.raw / "external" / filename

    if out_path.exists():
        logger.info(f"Holidays data already exists: {out_path.name}")
        return out_path

    logger.info(f"Generating holidays for years: {years}")

    holiday_list = []
    for year in years:
        us_holidays = holidays.US(years=[year], observed=True)
        for date in pd.date_range(f"{year}-01-01", f"{year}-12-31"):
            if date.date() in us_holidays:
                holiday_list.append({
                    "date": date.date(),
                    "holiday_name": us_holidays.get(date.date())
                })

    holidays_df = pd.DataFrame(holiday_list)
    holidays_df.to_csv(out_path, index=False)

    logger.info(f"Saved {out_path.name} ({len(holidays_df)} holidays)")
    return out_path
