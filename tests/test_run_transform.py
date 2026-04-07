from pathlib import Path

import nyc_mobility_friction.transformers.run_transform as run_transform_module


def test_iter_month_chunks_splits_partial_range():
    chunks = list(
        run_transform_module._iter_month_chunks(
            start_date="2025-01-01",
            end_date="2025-03-31",
        )
    )

    assert chunks == [
        ("2025-01-01", "2025-01-31"),
        ("2025-02-01", "2025-02-28"),
        ("2025-03-01", "2025-03-31"),
    ]


def test_run_transform_calls_taxi_month_by_month(monkeypatch):
    calls = []

    monkeypatch.setattr(run_transform_module, "setup_logger", lambda name: Path("dummy.log"))

    def fake_transform_taxi_zone_day(start_date, end_date, taxi_type, force):
        calls.append((start_date, end_date, taxi_type, force))
        return Path(f"{taxi_type}_{start_date}_{end_date}.parquet")

    monkeypatch.setattr(
        run_transform_module,
        "transform_taxi_zone_day",
        fake_transform_taxi_zone_day,
    )

    outputs = run_transform_module.run_transform(
        years=[],
        start_date="2025-01-01",
        end_date="2025-03-31",
        taxi_type="yellow",
        force=True,
    )

    assert len(outputs) == 3
    assert calls == [
        ("2025-01-01", "2025-01-31", "yellow", True),
        ("2025-02-01", "2025-02-28", "yellow", True),
        ("2025-03-01", "2025-03-31", "yellow", True),
    ]
