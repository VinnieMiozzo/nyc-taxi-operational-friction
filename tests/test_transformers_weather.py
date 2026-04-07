from types import SimpleNamespace

import pandas as pd
import pytest

import nyc_mobility_friction.transformers.weather as weather_transformer


def test_transform_weather_writes_expected_output(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "external"
    processed_dir = tmp_path / "data" / "processed" / "weather"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    raw_df = pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02", "2025-01-03"],
            "weather_code": [1, 2, 3],
            "temperature_2m_max": [50, 51, 49],
            "temperature_2m_min": [40, 39, 38],
            "temp_avg_f": [45, 45, 43.5],
            "precipitation_sum": [0.0, 0.2, 0.0],
            "snowfall_sum": [0.0, 0.0, 0.0],
            "precipitation_hours": [0, 2, 0],
            "wind_speed_10m_max": [10, 12, 8],
            "has_precip": [False, True, False],
            "has_snow": [False, False, False],
        }
    )
    raw_df.to_csv(raw_dir / "nyc_daily_weather_2025-01-01_2025-01-03.csv", index=False)

    monkeypatch.setattr(
        weather_transformer,
        "get_project_paths",
        lambda: SimpleNamespace(
            raw=tmp_path / "data" / "raw",
            processed=tmp_path / "data" / "processed",
        ),
    )

    out_path = weather_transformer.transform_weather(
        start_date="2025-01-01",
        end_date="2025-01-03",
        force=True,
    )

    assert out_path.exists()
    assert out_path.name == "weather_daily_2025-01-01_2025-01-03.csv"

    result = pd.read_csv(out_path)
    assert len(result) == 3
    assert list(result.columns) == [
        "date",
        "weather_code",
        "temperature_2m_max",
        "temperature_2m_min",
        "temp_avg_f",
        "precipitation_sum",
        "snowfall_sum",
        "precipitation_hours",
        "wind_speed_10m_max",
        "has_precip",
        "has_snow",
    ]


def test_transform_weather_invalid_range_raises():
    with pytest.raises(ValueError):
        weather_transformer.transform_weather(
            start_date="2025-01-03",
            end_date="2025-01-01",
        )
