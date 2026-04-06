from types import SimpleNamespace

import pandas as pd

import nyc_mobility_friction.transformers.taxi as taxi_transformer


def test_transform_taxi_zone_day_end_to_end_one_month(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "taxi"
    processed_dir = tmp_path / "data" / "processed" / "taxi"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    raw_df = pd.DataFrame(
        {
            "PULocationID": [10, 10, 10, 20],
            "trip_distance": [2.0, 4.0, 0.0, 3.0],
            "fare_amount": [12.0, 20.0, 15.0, 18.0],
            "tpep_pickup_datetime": [
                "2025-01-01 08:00:00",
                "2025-01-01 09:00:00",
                "2025-01-01 10:00:00",
                "2025-01-02 11:00:00",
            ],
            "tpep_dropoff_datetime": [
                "2025-01-01 08:10:00",
                "2025-01-01 09:20:00",
                "2025-01-01 10:15:00",
                "2025-01-02 11:15:00",
            ],
        }
    )

    raw_path = raw_dir / "yellow_tripdata_2025-01.parquet"
    raw_df.to_parquet(raw_path, index=False)

    monkeypatch.setattr(
        taxi_transformer,
        "get_project_paths",
        lambda: SimpleNamespace(
            raw=tmp_path / "data" / "raw",
            processed=tmp_path / "data" / "processed",
        ),
    )

    out_path = taxi_transformer.transform_taxi_zone_day(
        start_date="2025-01-01",
        end_date="2025-01-02",
        taxi_type="yellow",
        force=True,
    )

    assert out_path.exists()
    assert out_path.name == "yellow_taxi_zone_day_2025-01-01_2025-01-02.parquet"

    result = pd.read_parquet(out_path)

    assert list(result.columns) == [
        "pickup_zone_id",
        "pickup_date",
        "trip_count",
        "median_trip_duration_min",
        "p90_trip_duration_min",
        "median_trip_distance",
        "median_pace_min_per_mile",
        "p90_pace_min_per_mile",
        "total_fare_amount",
    ]

    assert len(result) == 2

    zone_10_day_1 = result.loc[
        (result["pickup_zone_id"] == 10)
        & (result["pickup_date"] == pd.Timestamp("2025-01-01")),
    ].iloc[0]

    assert zone_10_day_1["trip_count"] == 2
    assert zone_10_day_1["median_trip_duration_min"] == 15.0
    assert zone_10_day_1["median_trip_distance"] == 3.0
    assert zone_10_day_1["median_pace_min_per_mile"] == 5.0
    assert zone_10_day_1["total_fare_amount"] == 32.0

    zone_20_day_2 = result.loc[
        (result["pickup_zone_id"] == 20)
        & (result["pickup_date"] == pd.Timestamp("2025-01-02")),
    ].iloc[0]

    assert zone_20_day_2["trip_count"] == 1
    assert zone_20_day_2["median_trip_duration_min"] == 15.0
    assert zone_20_day_2["median_pace_min_per_mile"] == 5.0
