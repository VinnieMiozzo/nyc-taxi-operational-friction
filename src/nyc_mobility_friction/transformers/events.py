"""
Events transformer for the NYC Mobility Friction project.

This module converts raw permitted event records into a coarse daily
citywide event context dataset for downstream joins with taxi zone-day data.
"""

from pathlib import Path
import logging

import pandas as pd

from nyc_mobility_friction.paths import get_project_paths

logger = logging.getLogger(__name__)


def transform_events(
    start_date: str,
    end_date: str,
    force: bool = False,
) -> Path:
    """Transform raw permitted events into daily citywide event features.

    Args:
        start_date: Inclusive start date in YYYY-MM-DD format.
        end_date: Inclusive end date in YYYY-MM-DD format.
        force: Overwrite an existing processed file if True.

    Returns:
        Path to the saved processed events CSV file.
    """
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    if start_ts > end_ts:
        raise ValueError(f"start_date ({start_date}) must be <= end_date ({end_date})")

    paths = get_project_paths()

    in_path = paths.raw / "external" / f"nyc_permitted_events_{start_date}_{end_date}.csv"
    if not in_path.exists():
        raise FileNotFoundError(f"Missing raw events file: {in_path.name}")

    out_dir = paths.processed / "events"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"events_daily_{start_date}_{end_date}.csv"
    temp_path = out_path.with_suffix(".part.csv")

    if out_path.exists() and not force:
        logger.info(f"Using existing processed events file: {out_path.name}")
        return out_path

    logger.info(f"Transforming events data for {start_date} to {end_date}")

    df = pd.read_csv(in_path)

    if df.empty:
        out = pd.DataFrame(
            {
                "date": pd.date_range(start_ts, end_ts, freq="D"),
                "event_count": 0,
                "street_closure_event_count": 0,
            }
        )
        out.to_csv(temp_path, index=False)
        temp_path.replace(out_path)
        return out_path

    df["start_date_time"] = pd.to_datetime(df["start_date_time"], errors="coerce")
    df["end_date_time"] = pd.to_datetime(df["end_date_time"], errors="coerce")

    df = df.loc[df["start_date_time"].notna(), :].copy()
    df["date"] = df["start_date_time"].dt.normalize()

    df = df.loc[(df["date"] >= start_ts) & (df["date"] <= end_ts), :].copy()

    df["has_street_closure"] = df["street_closure_type"].notna()

    daily = (
        df.groupby("date", as_index=False)
        .agg(
            event_count=("event_id", "size"),
            street_closure_event_count=("has_street_closure", "sum"),
        )
    )

    full_dates = pd.DataFrame({"date": pd.date_range(start_ts, end_ts, freq="D")})
    out = full_dates.merge(daily, on="date", how="left").fillna(0)

    out["event_count"] = out["event_count"].astype(int)
    out["street_closure_event_count"] = out["street_closure_event_count"].astype(int)

    out.to_csv(temp_path, index=False)
    temp_path.replace(out_path)

    logger.info(f"Saved processed events file: {out_path.name} ({len(out):,} rows)")
    return out_path
