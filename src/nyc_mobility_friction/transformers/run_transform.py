":qa
Command-line orchestration for processed data transformation.

This module runs transformer jobs for the NYC Mobility Friction project.
For taxi data, long study windows are split into month-sized chunks so raw
trip data is not loaded for too many months at once.
"""

import argparse
import logging
import pandas as pd
from datetime import datetime, date
from calendar import monthrange
from pathlib import Path

from nyc_mobility_friction.extractors.utils import setup_logger
from nyc_mobility_friction.transformers.taxi import transform_taxi_zone_day
from nyc_mobility_friction.transformers.weather import transform_weather
from nyc_mobility_friction.transformers.events import transform_events
from nyc_mobility_friction.transformers.calendar import transform_calendar

logger = logging.getLogger(__name__)


def _validate_months(months: list[int]) -> list[int]:
    """Validate and normalize month list."""
    months = sorted(set(months))
    invalid = [m for m in months if m < 1 or m > 12]

    if invalid:
        raise ValueError(
            f"Invalid month(s): {invalid}. Months must be between 1 and 12."
        )

    return months


def _month_start_end(year: int, month: int) -> tuple[str, str]:
    """Return first and last day of a given month as YYYY-MM-DD strings."""
    last_day = monthrange(year, month)[1]
    start_date = f"{year:04d}-{month:02d}-01"
    end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
    return start_date, end_date


def _iter_month_chunks(start_date: str, end_date: str):
    """Split an arbitrary date range into month-sized chunks."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if start > end:
        raise ValueError(
            f"start_date ({start_date}) must be <= end_date ({end_date})"
        )

    current = date(start.year, start.month, 1)

    while current <= end:
        year = current.year
        month = current.month
        month_last_day = monthrange(year, month)[1]

        chunk_start = max(start, date(year, month, 1))
        chunk_end = min(end, date(year, month, month_last_day))

        yield chunk_start.isoformat(), chunk_end.isoformat()

        if month == 12:
            current = date(year + 1, 1, 1)
        else:
            current = date(year, month + 1, 1)


def _build_transform_ranges(
    years: list[int],
    months: list[int],
    start_date: str | None,
    end_date: str | None,
) -> list[tuple[str, str]]:
    """Build monthly transformation ranges."""
    if start_date is not None and end_date is not None:
        return list(_iter_month_chunks(start_date, end_date))

    if start_date is None and end_date is None:
        ranges = []
        for year in sorted(set(years)):
            for month in months:
                ranges.append(_month_start_end(year, month))
        return ranges

    raise ValueError("Provide both --start-date and --end-date, or neither.")


def run_transform(
    years: list[int],
    months: list[int] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    taxi_type: str = "yellow",
    force: bool = False,
) -> list[Path]:
    """
    Run transformation jobs for processed datasets.

    For taxi data, transformation is intentionally chunked month by month
    to reduce memory pressure from raw trip files.

    Args:
        years: Years to transform when explicit dates are not provided.
        months: Months to transform when explicit dates are not provided.
        start_date: Optional explicit start date for taxi transformation.
        end_date: Optional explicit end date for taxi transformation.
        taxi_type: Taxi source to transform.
        force: Rebuild processed files even if they already exist.

    Returns:
        List of written processed file paths.
    """
    log_path = setup_logger("run_transform")
    logger.info(f"Writing logs to: {log_path}")

    if not years and (start_date is None or end_date is None):
        raise ValueError(
            "You must provide years unless using --start-date and --end-date."
        )

    years = sorted(set(years)) if years else []
    months = _validate_months(months or list(range(1, 13)))

    logger.info("Starting transformation pipeline")
    logger.info(f"Years: {years}")
    logger.info(f"Months: {months}")
    logger.info(f"Taxi type: {taxi_type}")
    logger.info(f"Force rebuild: {force}")

    ranges = _build_transform_ranges(
        years=years,
        months=months,
        start_date=start_date,
        end_date=end_date,
    )

    logger.info(
        "Taxi transformation will run month by month to limit memory usage "
        "when reading raw trip files."
    )
    logger.info(f"Transformation ranges: {ranges}")

    outputs: list[Path] = []

    logger.info("=== Taxi Zone-Day Transformation ===")
    for chunk_start, chunk_end in ranges:
        logger.info(f"Transforming taxi data for {chunk_start} to {chunk_end}")

        out_path = transform_taxi_zone_day(
            start_date=chunk_start,
            end_date=chunk_end,
            taxi_type=taxi_type,
            force=force,
        )
        outputs.append(out_path)

    logger.info("=== Weather Transformation ===")
    for chunk_start, chunk_end in ranges:
        transform_weather(
            start_date=chunk_start,
            end_date=chunk_end,
            force=force,
        )

    logger.info("=== Events Transformation ===")
    for chunk_start, chunk_end in ranges:
        transform_events(
            start_date=chunk_start,
            end_date=chunk_end,
            force=force,
        )

    logger.info("=== Calendar Transformation ===")
    calendar_years = sorted(
        {
            pd.Timestamp(chunk_start).year
            for chunk_start, chunk_end in ranges
        }
        | {
            pd.Timestamp(chunk_end).year
            for chunk_start, chunk_end in ranges
        }
    )
    transform_calendar(years=calendar_years, force=force)

    logger.info(
        f"Transformation completed at {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    logger.info(f"Created {len(outputs)} processed file(s)")

    return outputs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run data transformers for the NYC Mobility Friction project."
    )

    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[],
        help="Years to transform when explicit dates are not provided.",
    )

    parser.add_argument(
        "--months",
        nargs="+",
        type=int,
        default=list(range(1, 13)),
        help="Months to transform when explicit dates are not provided.",
    )

    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional explicit start date for taxi transformation (YYYY-MM-DD).",
    )

    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional explicit end date for taxi transformation (YYYY-MM-DD).",
    )

    parser.add_argument(
        "--taxi-type",
        default="yellow",
        choices=["yellow", "green"],
        help="Taxi type to transform (default: yellow).",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild processed files even if they already exist.",
    )

    args = parser.parse_args()

    run_transform(
        years=args.years,
        months=args.months,
        start_date=args.start_date,
        end_date=args.end_date,
        taxi_type=args.taxi_type,
        force=args.force,
    )
