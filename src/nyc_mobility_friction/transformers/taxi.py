"""
Taxi transformer for the NYC Mobility Friction project.

This module converts raw monthly TLC taxi trip files into a processed
pickup-zone-by-day dataset for analysis. It enforces the study window
with pickup timestamps before aggregation, filters invalid trips, and
builds robust daily zone metrics such as median duration and pace.

The transformer is designed for decision support, not causal inference.
Its output is intended to support downstream panel construction and EDA.
"""

from __future__ import annotations

from pathlib import Path
import logging 

import pandas as pd 

from nyc_mobility_friction.paths import get_project_paths 

logger = logging.getLogger(__name__)

VALID_TAXI_TYPES = {"yellow", "green"}

RAW_COLUMNS = [
        "PULocationID",
        "trip_distance",
        "fare_amount",
        ]

DATETIME_COLUMNS = {
        "yellow": {
            "pickup": "tpep_pickup_datetime",
            "dropoff": "tpep_dropoff_datetime",
            },
        "greem": {
            "pickup": "lpep_pickup_datetime",
            "dropoff": "lpep_dropoff_datetime",
            },
        }


def _month_starts(start_date: str, end_date: str) -> list[str]:
    """Return month starts covering an inclusive date range."""
    start_ts = pd.Timestamp(start_date).to_period("M")
    end_ts = pd.Timestamp(end_date).to_period("M")
    return [str(period) for period in pd.period_range(start_ts, end_ts, freq = "M")]

def _build_raw_taxi_paths(
        start_date: str,
        end_date: str,
        taxi_type: str,
        ) -> list[Path]:
    """Build raw parquet paths for all months intersecting the study window."""
    if taxi_type not in VALID_TAXI_TYPES:
        raise ValueError(f"Invalid taxi_type: {taxi_type}")

    paths = get_project_paths()
    months = _month_starts(start_date, end_date)

    raw_paths = [
            paths.raw / "taxi" / f"{taxi_type}_tripdata_{month}.parquet"
            for month in months
            ]

    missing = [path.name for path in raw_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(
                "Missing raw taxi files. Run extraction first for: " + ", ".join(
                    missing
                    )
                )

    return raw_paths

def _load_raw_taxi(
        start_date: str,
        end_date: str,
        taxi_type: str,
        ) -> pd.DataFrame:
    """Load raw taxi parquet files for the requested study window."""
    dt_cols = DATETIME_COLUMNS[taxi_type]
    columns = RAW_COLUMNS + [dt_cols["pickup"], dt_cols["dropoff"]]

    frames = []
    for path in _build_raw_taxi_paths(start_date, end_date, taxi_type=taxi_type):
        logger.info(f"Loading raw taxi file: {path.name}")
        df = pd.read_parquet(path, columns=columns)

        df = df.rename(
                columns={
                    dt_cols["pickup"]: "pickup_datetime",
                    dt_cols["dropoff"]: "dropoff_datetime",
                    }
                )
        frames.append(df)

    return pd.concat(frames, ignore_index=True)

def clean_taxi_trips(
        df: pd.DataFrame,
        start_date: str,
        end_date: str,
        ) -> pd.DataFrame:
    """Apply study-window filtering and invalid-trip filtering before aggregation."""
    start_ts = pd.Timestamp(start_date)
    end_exclsuive_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)

    df = df.copy()

    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

    # Enforce study window using pickup datetime before aggreggation
    df = df[
        (df["dropoff_datetime"] >= start_ts)
        & (df["dropoff_datetime"] < end_exclsuive_ts)
            ].copy()
    
    print(df)

    df["trip_duration_min"] = (
            (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60
            )

    valid_mask = (
            df["pickup_datetime"].notna()
            & df["dropoff_datetime"].notna()
            & df["PULocationID"].notna()
            & (df["trip_distance"] > 0)
            & (df["trip_duration_min"] > 0)
            & (df["fare_amount"] > 0)
            )

    df = df.loc[valid_mask, :].copy()

    return df

def add_taxi_trip_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add trip-level features used for robust daily zone aggregation."""
    df = df.copy()

    df["pickup_zone_id"] = df["PULocationID"].astype("int64")
    df["pickup_date"] = df["pickup_datetime"].dt.normalize()
    df["pace_min_per_mile"] = df["trip_duration_min"] / df["trip_distance"]

    # Optional later:
    # df = df[df["pickup_zone_id"].between(1, 263)].copy()

    return df

def aggregate_taxi_zone_day(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregated cleaned taxi trips to pickup zone x day."""
    zone_day = (
            df.groupby(["pickup_zone_id", "pickup_date"], as_index=False)
            .agg(
                trip_count=("pickup_zone_id", "size"),
                median_trip_duration_min=("trip_duration_min", "median"),
                p90_trip_duration_min=("trip_duration_min", lambda s: s.quantile(0.9)),
                median_trip_distance=("trip_distance", "median"),
                median_pace_min_per_mile=("pace_min_per_mile", "median"),
                p90_pace_min_per_mile=("pace_min_per_mile", lambda s: s.quantile(0.90)),
                total_fare_amount=("fare_amount", "sum"),
                )
            .sort_values(["pickup_date", "pickup_zone_id"])
            .reset_index(drop=True)
            )

    return zone_day

def transform_taxi_zone_day(
        start_date: str,
        end_date: str,
        taxi_type: str = "yellow",
        force: bool = False,
        ) -> Path:
    """Build processed taxi zone-day metrics for the requested study window."""
    if pd.Timestamp(start_date) > pd.Timestamp(end_date):
        raise ValueError(f"start_date ({start_date}) must be <= end_date ({end_date})")
    if taxi_type not in VALID_TAXI_TYPES:
        raise ValueError(f"Invalid taxi_type: {taxi_type}. Expected one of {VALID_TAXI_TYPES}.")

    paths = get_project_paths()
    out_dir = paths.processed / "taxi"
    out_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{taxi_type}_taxi_zone_day_{start_date}_{end_date}.parquet"
    out_path = out_dir / filename
    temp_path = out_path.with_suffix(".part.parquet")

    if out_path.exists() and not force:
        logger.info(f"Using existing processed taxi file: {out_path.name}")
        return out_path

    logger.info(f"Transforming taxi data for {start_date} to {end_date}")

    raw_df = _load_raw_taxi(
        start_date=start_date,
        end_date=end_date,
        taxi_type=taxi_type,
    )
    clean_df = clean_taxi_trips(
        df=raw_df,
        start_date=start_date,
        end_date=end_date,
    )
    feature_df = add_taxi_trip_features(clean_df)
    zone_day_df = aggregate_taxi_zone_day(feature_df)

    zone_day_df.to_parquet(temp_path, index=False)
    temp_path.replace(out_path)

    logger.info(f"Saved processed taxi zone-day file: {out_path.name} ({len(zone_day_df):,} rows)")
    return out_path
