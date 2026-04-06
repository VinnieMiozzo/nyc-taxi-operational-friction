from types import SimpleNamespace

import pandas as pd
import pytest

import nyc_mobility_friction.extractors.weather as weather_module
import nyc_mobility_friction.extractors.calendar as calendar_module


class DummyResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class DummySession:
    def __init__(self, payload: dict):
        self.payload = payload
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        return DummyResponse(self.payload)


def test_extract_weather_writes_expected_file(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    external_dir = raw_dir / "external"

    monkeypatch.setattr(
        weather_module,
        "get_project_paths",
        lambda: SimpleNamespace(raw=raw_dir),
    )
    monkeypatch.setattr(
        weather_module,
        "ensure_external_dirs",
        lambda: external_dir.mkdir(parents=True, exist_ok=True),
    )

    payload = {
        "daily": {
            "time": ["2025-01-01", "2025-01-02"],
            "weather_code": [1, 2],
            "temperature_2m_max": [50.0, 52.0],
            "temperature_2m_min": [40.0, 41.0],
            "precipitation_sum": [0.0, 0.2],
            "snowfall_sum": [0.0, 0.0],
            "precipitation_hours": [0.0, 2.0],
            "wind_speed_10m_max": [10.0, 12.0],
        }
    }
    session = DummySession(payload)
    monkeypatch.setattr(weather_module, "make_session", lambda: session)

    path = weather_module.extract_weather(
        start_date="2025-01-01",
        end_date="2025-01-02",
        force=False,
    )

    assert path.exists()
    assert path.name == "nyc_daily_weather_2025-01-01_2025-01-02.csv"
    assert session.calls == 1

    df = pd.read_csv(path)
    assert len(df) == 2
    assert "temp_avg_f" in df.columns
    assert "has_precip" in df.columns
    assert "has_snow" in df.columns
    assert df.loc[0, "temp_avg_f"] == 45.0
    assert bool(df.loc[1, "has_precip"]) is True


def test_extract_weather_reuses_existing_file_when_not_force(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    external_dir = raw_dir / "external"
    external_dir.mkdir(parents=True, exist_ok=True)

    out_path = external_dir / "nyc_daily_weather_2025-01-01_2025-01-02.csv"
    out_path.write_text("date\n2025-01-01\n", encoding="utf-8")

    monkeypatch.setattr(
        weather_module,
        "get_project_paths",
        lambda: SimpleNamespace(raw=raw_dir),
    )
    monkeypatch.setattr(weather_module, "ensure_external_dirs", lambda: None)

    def should_not_be_called():
        raise AssertionError("make_session should not be called when cache is reused")

    monkeypatch.setattr(weather_module, "make_session", should_not_be_called)

    path = weather_module.extract_weather(
        start_date="2025-01-01",
        end_date="2025-01-02",
        force=False,
    )

    assert path == out_path
    assert path.read_text(encoding="utf-8") == "date\n2025-01-01\n"


def test_extract_weather_force_rebuilds_existing_file(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    external_dir = raw_dir / "external"
    external_dir.mkdir(parents=True, exist_ok=True)

    out_path = external_dir / "nyc_daily_weather_2025-01-01_2025-01-02.csv"
    out_path.write_text("old_data\n1\n", encoding="utf-8")

    monkeypatch.setattr(
        weather_module,
        "get_project_paths",
        lambda: SimpleNamespace(raw=raw_dir),
    )
    monkeypatch.setattr(weather_module, "ensure_external_dirs", lambda: None)

    payload = {
        "daily": {
            "time": ["2025-01-01"],
            "weather_code": [1],
            "temperature_2m_max": [50.0],
            "temperature_2m_min": [40.0],
            "precipitation_sum": [0.0],
            "snowfall_sum": [0.0],
            "precipitation_hours": [0.0],
            "wind_speed_10m_max": [10.0],
        }
    }
    session = DummySession(payload)
    monkeypatch.setattr(weather_module, "make_session", lambda: session)

    path = weather_module.extract_weather(
        start_date="2025-01-01",
        end_date="2025-01-02",
        force=True,
    )

    df = pd.read_csv(path)
    assert len(df) == 1
    assert "old_data" not in df.columns
    assert session.calls == 1


def test_extract_weather_invalid_date_range_raises():
    with pytest.raises(ValueError):
        weather_module.extract_weather(
            start_date="2025-01-03",
            end_date="2025-01-01",
        )


def test_extract_holidays_writes_expected_file(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    external_dir = raw_dir / "external"

    monkeypatch.setattr(
        calendar_module,
        "get_project_paths",
        lambda: SimpleNamespace(raw=raw_dir),
    )
    monkeypatch.setattr(
        calendar_module,
        "ensure_external_dirs",
        lambda: external_dir.mkdir(parents=True, exist_ok=True),
    )

    path = calendar_module.extract_holidays(years=[2025], force=True)

    assert path.exists()
    assert path.name == "holidays_2025_2025.csv"

    df = pd.read_csv(path)
    assert {"date", "holiday_name"}.issubset(df.columns)
    assert len(df) > 0
