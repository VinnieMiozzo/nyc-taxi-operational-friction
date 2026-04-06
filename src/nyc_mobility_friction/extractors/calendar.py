"""
Holiday extractor for the NYC Mobility Friction project.

This module generates a raw CSV of observed US holidays for the
requested years and saves it under data/raw/external/.
"""


from pathlib import Path
import pandas as pd
import holidays

from .utils import (
    ensure_external_dirs,
)

from nyc_mobility_friction.paths import get_project_paths

import logging
logger = logging.getLogger(__name__)


def extract_holidays(years: list[int], force: bool = False) -> Path:
    """Generate observed US holidays for the requested years.

    Args:
        years: List of four-digit years, such as [2024, 2025].
        force: Overwrite an existing file if True.

    Returns:
        Path to the saved holidays CSV file.
    """
    ensure_external_dirs()
    paths = get_project_paths()

    filename = f"holidays_{years[0]}_{years[-1]}.csv"
    out_path = paths.raw / "external" / filename

    if out_path.exists() and not force:
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
    temp_path = out_path.with_suffix(out_path.suffix + ".path")

    holidays_df.to_csv(temp_path, index=False)
    temp_path.replace(out_path)

    logger.info(f"Saved {out_path.name} ({len(holidays_df)} holidays)")
    return out_path
