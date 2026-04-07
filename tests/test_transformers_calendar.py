from types import SimpleNamespace

import pandas as pd

import nyc_mobility_friction.transformers.calendar as calendar_transformer


def test_transform_calendar_builds_daily_flags(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "external"
    processed_dir = tmp_path / "data" / "processed" / "calendar"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    holidays_df = pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-12-25"],
            "holiday_name": ["New Year's Day", "Christmas Day"],
        }
    )
    holidays_df.to_csv(raw_dir / "holidays_2025_2025.csv", index=False)

    monkeypatch.setattr(
        calendar_transformer,
        "get_project_paths",
        lambda: SimpleNamespace(
            raw=tmp_path / "data" / "raw",
            processed=tmp_path / "data" / "processed",
        ),
    )

    out_path = calendar_transformer.transform_calendar(years=[2025], force=True)

    assert out_path.exists()
    assert out_path.name == "calendar_daily_2025_2025.csv"

    result = pd.read_csv(out_path)
    assert len(result) == 365
    assert {"date", "holiday_name", "is_holiday", "day_of_week", "day_of_week_num", "is_weekend"}.issubset(result.columns)

    jan_1 = result.loc[result["date"] == "2025-01-01"].iloc[0]
    assert jan_1["holiday_name"] == "New Year's Day"
    assert bool(jan_1["is_holiday"]) is True
