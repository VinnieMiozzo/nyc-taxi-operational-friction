"""
NYC Mobility Friction Data Extractor

Downloads NYC Permitted Events from the official Socrata API.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from requests.adapters import HTTPAdapter
from sodapy import Socrata
from urllib3.util.retry import Retry

from .utils import setup_logger, ensure_external_dirs
from nyc_mobility_friction.paths import get_project_paths

logger = setup_logger(__name__)

DATASET_ID = "bkfu-528j"

VALID_COLUMNS = [
    "event_id",
    "event_name",
    "start_date_time",
    "end_date_time",
    "event_agency",
    "event_type",
    "event_borough",
    "event_location",
    "street_closure_type",
    "community_board",
    "police_precinct",
]


def make_socrata_client(domain: str) -> Socrata:
    client = Socrata(
        domain,
        os.getenv("SOCRATA_APP_TOKEN"),
        timeout=120,
    )

    retry = Retry(
        total=8,
        connect=8,
        read=8,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry)
    client.session.mount("http://", adapter)
    client.session.mount("https://", adapter)

    return client


def extract_events(
    start_date: str = "2025-01-01",
    end_date: str = "2025-03-31",
    force: bool = False,
    limit: int = 100_000,
) -> Path:
    """
    Download NYC permitted events from the Socrata API.

    Strategy:
    - query Socrata by year using date_extract_y(start_date_time)
    - page through results with stable ordering
    - filter the exact requested date range locally in pandas
    - write incrementally to CSV

    Args:
        start_date: inclusive YYYY-MM-DD
        end_date: inclusive YYYY-MM-DD
        force: overwrite existing file if True
        limit: Socrata page size

    Returns:
        Path to the saved CSV file.
    """
    ensure_external_dirs()
    paths = get_project_paths()

    filename = f"nyc_permitted_events_{start_date[:7]}_{end_date[:7]}.csv"
    out_path = paths.raw / "external" / filename
    temp_path = out_path.with_suffix(".part.csv")

    if out_path.exists() and not force:
        logger.info(f"Events data already exists: {out_path.name}")
        return out_path

    if temp_path.exists():
        temp_path.unlink()

    start_ts = pd.Timestamp(start_date)
    end_exclusive_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)

    years = range(start_ts.year, end_exclusive_ts.year + 1)
    select_clause = ",".join(VALID_COLUMNS)

    client = make_socrata_client("data.cityofnewyork.us")

    logger.info(f"Downloading NYC Permitted Events -> {start_date} to {end_date}")

    first_chunk = True
    total_fetched = 0
    total_written = 0

    try:
        for year in years:
            where_clause = (
                    f"start_date_time >= '{start_date}' "
                    f"AND start_date_time < '{end_date}'"
                )
            logger.info(f"Processing year {year} with WHERE: {where_clause}")

            try:
                count_result = client.get(
                    DATASET_ID,
                    select="count(*) as n",
                    where=where_clause,
                )
                expected_rows = int(count_result[0]["n"])
                logger.info(f"Expected rows for {start_date} to {end_date}: {expected_rows:,}")
            except Exception as e:
                logger.warning(f"Could not get count for {start_date} to {end_date}: {e}")
                expected_rows = None

            offset = 0

            while True:
                batch = client.get(
                    DATASET_ID,
                    select=select_clause,
                    where=where_clause,
                    order=":id",
                    limit=limit,
                    offset=offset,
                )

                if not batch:
                    break

                df = pd.DataFrame.from_records(batch)
                batch_size = len(df)
                total_fetched += batch_size

                # Local exact date-range filter
                df["start_date_time"] = pd.to_datetime(
                    df["start_date_time"],
                    errors="coerce",
                )

                df = df[
                    (df["start_date_time"] >= start_ts)
                    & (df["start_date_time"] < end_exclusive_ts)
                ].copy()

                if not df.empty:
                    df.to_csv(
                        temp_path,
                        mode="w" if first_chunk else "a",
                        header=first_chunk,
                        index=False,
                    )
                    total_written += len(df)
                    first_chunk = False

                offset += limit

                if expected_rows is None:
                    logger.info(
                        f"Year {year} | fetched={total_fetched:,} | written={total_written:,}"
                    )
                else:
                    logger.info(
                        f"Year {year} | fetched this query up to offset {offset:,} / {expected_rows:,} | total_written={total_written:,}"
                    )

                if batch_size < limit:
                    break

        if first_chunk:
            # no rows matched; create an empty file with headers
            pd.DataFrame(columns=VALID_COLUMNS).to_csv(out_path, index=False)
            logger.info(f"No rows found for requested range. Saved empty file: {out_path.name}")
            return out_path

        temp_df = pd.read_csv(temp_path)

        # Light safeguard against accidental duplicates
        dedupe_cols = ["event_id", "start_date_time", "end_date_time"]
        if all(col in temp_df.columns for col in dedupe_cols):
            before = len(temp_df)
            temp_df = temp_df.drop_duplicates(subset=dedupe_cols)
            removed = before - len(temp_df)
            if removed > 0:
                logger.warning(f"Removed {removed:,} duplicate rows after download")

        temp_df.to_csv(out_path, index=False)
        temp_path.unlink(missing_ok=True)

        logger.info(
            f"Saved {out_path.name} | fetched={total_fetched:,} raw rows | final={len(temp_df):,} rows"
        )
        return out_path

    except Exception as e:
        logger.error(f"Failed to download events: {e}")
        temp_path.unlink(missing_ok=True)
        raise
