from pathlib import Path
import re


RAW_EXTERNAL_DIR = Path("data/raw/external")


def rename_legacy_weather_files() -> None:
    """
    Rename legacy weather files from:
        nyc_daily_weather_YYYY-MM_YYYY-MM.csv
    to:
        nyc_daily_weather_YYYY-MM-01_YYYY-MM-lastday.csv
    """
    pattern = re.compile(r"^nyc_daily_weather_(\d{4}-\d{2})_(\d{4}-\d{2})\.csv$")

    for path in RAW_EXTERNAL_DIR.glob("nyc_daily_weather_*.csv"):
        match = pattern.match(path.name)
        if not match:
            continue

        start_month, end_month = match.groups()

        start_date = f"{start_month}-01"

        # crude but fine for monthly legacy files: use pandas-free stdlib
        year, month = map(int, end_month.split("-"))
        if month == 12:
            next_year, next_month = year + 1, 1
        else:
            next_year, next_month = year, month + 1

        from datetime import date, timedelta
        end_date = (date(next_year, next_month, 1) - timedelta(days=1)).isoformat()

        new_name = f"nyc_daily_weather_{start_date}_{end_date}.csv"
        new_path = path.with_name(new_name)

        if new_path.exists():
            print(f"SKIP exists: {new_path.name}")
            continue

        path.rename(new_path)
        print(f"RENAMED: {path.name} -> {new_path.name}")


def rename_legacy_events_files() -> None:
    """
    Rename legacy events files from:
        nyc_permitted_events_YYYY-MM_YYYY-MM.csv
    to:
        nyc_permitted_events_YYYY-MM-01_YYYY-MM-lastday.csv
    """
    pattern = re.compile(r"^nyc_permitted_events_(\d{4}-\d{2})_(\d{4}-\d{2})\.csv$")

    for path in RAW_EXTERNAL_DIR.glob("nyc_permitted_events_*.csv"):
        match = pattern.match(path.name)
        if not match:
            continue

        start_month, end_month = match.groups()

        start_date = f"{start_month}-01"

        year, month = map(int, end_month.split("-"))
        if month == 12:
            next_year, next_month = year + 1, 1
        else:
            next_year, next_month = year, month + 1

        from datetime import date, timedelta
        end_date = (date(next_year, next_month, 1) - timedelta(days=1)).isoformat()

        new_name = f"nyc_permitted_events_{start_date}_{end_date}.csv"
        new_path = path.with_name(new_name)

        if new_path.exists():
            print(f"SKIP exists: {new_path.name}")
            continue

        path.rename(new_path)
        print(f"RENAMED: {path.name} -> {new_path.name}")


if __name__ == "__main__":
    if not RAW_EXTERNAL_DIR.exists():
        raise FileNotFoundError(f"Missing directory: {RAW_EXTERNAL_DIR}")

    rename_legacy_weather_files()
    rename_legacy_events_files()
