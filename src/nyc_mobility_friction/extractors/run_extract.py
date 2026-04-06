import argparse
import logging
import requests
from datetime import datetime, date
from calendar import monthrange

from nyc_mobility_friction.extractors.taxi import download_taxi_month
from nyc_mobility_friction.extractors.events import extract_events
from nyc_mobility_friction.extractors.weather import extract_weather
from nyc_mobility_friction.extractors.calendar import extract_holidays
from nyc_mobility_friction.extractors.utils import setup_logger

logger = logging.getLogger(__name__)


def _validate_months(months: list[int]) -> list[int]:
    """Validate and normalize month list."""
    months = sorted(set(months))
    invalid = [m for m in months if m < 1 or m > 12]
    if invalid:
        raise ValueError(f"Invalid month(s): {invalid}. Months must be between 1 and 12.")
    return months


def _month_start_end(year: int, month: int) -> tuple[str, str]:
    """Return first and last day of a given month as YYYY-MM-DD strings."""
    last_day = monthrange(year, month)[1]
    start_date = f"{year:04d}-{month:02d}-01"
    end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
    return start_date, end_date


def _iter_month_chunks(start_date: str, end_date: str):
    """
    Split an arbitrary date range into month-sized chunks.

    Example:
        2025-01-15 to 2025-03-10
        ->
        2025-01-15 to 2025-01-31
        2025-02-01 to 2025-02-28
        2025-03-01 to 2025-03-10
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if start > end:
        raise ValueError(f"start_date ({start_date}) must be <= end_date ({end_date})")

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


def _build_monthly_ranges(
    years: list[int],
    months: list[int],
    start_date: str | None,
    end_date: str | None,
) -> list[tuple[str, str]]:
    """
    Build the list of monthly ranges for events/weather.

    Behavior:
    - If both start_date and end_date are provided, split that range month by month.
    - If neither is provided, use the explicit years/months requested.
    - If only one is provided, raise an error.
    """
    if start_date is not None and end_date is not None:
        return list(_iter_month_chunks(start_date, end_date))

    if start_date is None and end_date is None:
        ranges = []
        for year in sorted(set(years)):
            for month in months:
                ranges.append(_month_start_end(year, month))
        return ranges

    raise ValueError("Provide both --start-date and --end-date, or neither.")


def run_full_extraction(
    years: list[int],
    months: list[int] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    force: bool = False,
) -> None:
    """
    Run the complete data extraction pipeline.

    Args:
        years: Years to download taxi data for, e.g. [2024, 2025].
        months: Months to download, e.g. [1, 2, 3]. Defaults to all 12 months.
        start_date: Optional explicit start date for events/weather (YYYY-MM-DD).
        end_date: Optional explicit end date for events/weather (YYYY-MM-DD).
        force: Re-download / rebuild raw files even if they already exist.
    """
    log_path = setup_logger("run_extract")
    logger.info(f"Writing logs to: {log_path}")

    if not years:
        raise ValueError("You must provide at least one year.")

    years = sorted(set(years))
    months = _validate_months(months or list(range(1, 13)))

    logger.info("Starting full extraction for NYC Mobility Friction project")
    logger.info(f"Years: {years}")
    logger.info(f"Months: {months}")
    logger.info(f"Force rebuild: {force}")

    if start_date is not None and end_date is not None:
        logger.info(
            "Explicit date range applies to events/weather only. "
            "Taxi extraction remains month-based; exact taxi study-window filtering happens later during cleaning."
        )

    monthly_ranges = _build_monthly_ranges(
        years=years,
        months=months,
        start_date=start_date,
        end_date=end_date,
    )

    logger.info(f"Monthly context ranges: {monthly_ranges}")

    logger.info("=== Taxi Data ===")
    for year in years:
        for month in months:
            try:
                logger.info(f"Getting taxi data for {year}-{month:02d}")
                download_taxi_month(
                    year=year,
                    month=month,
                    taxi_type="yellow",
                    force=force,
                )
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 403:
                    logger.warning(f"Data not yet available for {year}-{month:02d}. Skipping.")
                    continue
                raise

    logger.info("=== Permitted Events ===")
    for chunk_start, chunk_end in monthly_ranges:
        logger.info(f"Getting events data from {chunk_start} to {chunk_end}")
        extract_events(
            start_date=chunk_start,
            end_date=chunk_end,
            force=force,
        )

    logger.info("=== Weather Data ===")
    for chunk_start, chunk_end in monthly_ranges:
        logger.info(f"Getting weather data from {chunk_start} to {chunk_end}")
        extract_weather(
            start_date=chunk_start,
            end_date=chunk_end,
            force=force,
        )

    logger.info("=== Holidays ===")
    logger.info(f"Getting holidays for years: {years}")
    extract_holidays(years=years, force=force)

    logger.info(f"Extraction completed at {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    logger.info("Raw data location -> data/raw/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download all raw data for the NYC Mobility Friction project."
    )

    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2025],
        help="Years to download taxi data for (default: 2025)",
    )

    parser.add_argument(
        "--months",
        nargs="+",
        type=int,
        default=list(range(1, 13)),
        help="Months to download (default: 1..12)",
    )

    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional start date for events/weather (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional end date for events/weather (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download or rebuild files even if they already exist.",
    )

    args = parser.parse_args()

    run_full_extraction(
        years=args.years,
        months=args.months,
        start_date=args.start_date,
        end_date=args.end_date,
        force=args.force,
    )
