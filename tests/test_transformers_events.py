from types import SimpleNamespace

import pandas as pd
import pytest

import nyc_mobility_friction.transformers.events as events_transformer


def test_transform_events_builds_daily_citywide_features(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "external"
    processed_dir = tmp_path / "data" / "processed" / "events"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    raw_df = pd.DataFrame(
        {
            "event_id": [1, 2, 3],
            "start_date_time": [
                "2025-01-01 10:00:00",
                "2025-01-01 18:00:00",
                "2025-01-03 09:00:00",
            ],
            "end_date_time": [
                "2025-01-01 12:00:00",
                "2025-01-01 21:00:00",
                "2025-01-03 11:00:00",
            ],
            "street_closure_type": ["Full", None, "Partial"],
        }
    )
    raw_df.to_csv(raw_dir / "nyc_permitted_events_2025-01-01_2025-01-03.csv", index=False)

    monkeypatch.setattr(
        events_transformer,
        "get_project_paths",
        lambda: SimpleNamespace(
            raw=tmp_path / "data" / "raw",
            processed=tmp_path / "data" / "processed",
        ),
    )

    out_path = events_transformer.transform_events(
        start_date="2025-01-01",
        end_date="2025-01-03",
        force=True,
    )

    assert out_path.exists()
    assert out_path.name == "events_daily_2025-01-01_2025-01-03.csv"

    result = pd.read_csv(out_path)
    assert len(result) == 3

    day_1 = result.loc[result["date"] == "2025-01-01"].iloc[0]
    assert day_1["event_count"] == 2
    assert day_1["street_closure_event_count"] == 1

    day_2 = result.loc[result["date"] == "2025-01-02"].iloc[0]
    assert day_2["event_count"] == 0
    assert day_2["street_closure_event_count"] == 0

    day_3 = result.loc[result["date"] == "2025-01-03"].iloc[0]
    assert day_3["event_count"] == 1
    assert day_3["street_closure_event_count"] == 1


def test_transform_events_invalid_range_raises():
    with pytest.raises(ValueError):
        events_transformer.transform_events(
            start_date="2025-01-03",
            end_date="2025-01-01",
        )
