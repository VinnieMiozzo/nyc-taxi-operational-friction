import marimo

__generated_with = "manual-conversion"
app = marimo.App()

@app.cell
def _():
    import marimo as mo
    return (mo,)

@app.cell(hide_code=True)
def _(mo):
    mo.md('# 02 External Context Review: Weather, Events & Holidays\n\n**NYC Taxi Operational Friction**  \n**Portfolio Project**  \n**Author:** Vinicius Miozzo  \n**Date:** April 2026\n\n---\n\n## Objective\n\nCreate a clean daily context dataset that captures broad factors which may affect taxi activity across time, including:\n\n- weather  \n- permitted-event intensity  \n- holiday effects  \n\nThis notebook is **not** estimating causal effects and is not yet producing a final mobility-friction score.\n\n---\n\n## Setup & Imports\n\n---\n\n## Study window\n\nAll source data in this notebook is filtered to the Jan-Mar 2025 study window.\n\nThe taxi data originally contained a small number of out-of-window timestamps, so a strict pickup-date filter is applied before any aggregation.\n\n---\n\n---\n\n## 1. Load Taxi Data (3 Months) – Reference\n\n---\n\n----\n\n## 2. Load Taxi Zone Lookup (Official TLC)\n\n---\n\n---\n\n## 3. Load NYC Permitted Events (Historical)\n\nDirect download from NYC Open Data (covers Jan–Mar 2025).\n\n---\n\n---\n\n## 4. Weather Data (Jan–Mar 2025)\n\n---\n\n---\n\n## 5. Holidays (US Federal + NYC Observances)\n\n---\n\n---\n## 6. Daily context table\n\nThe goal of this notebook is to create one clean daily table that can later be merged into zone-day taxi analysis.\n\nThis table contains:\n- daily taxi volume for context\n- citywide permitted-event intensity\n- citywide weather\n- holiday flags\n\n---\n\n---\n\n## 7. Sanity plots\n\nThese plots are intended as basic checks that the merged daily context table behaves as expected. They are not formal inference.\n\n---\n\n## What this notebook established\n\nThis notebook successfully produced a clean daily context table for Jan–Mar 2025.Main takeaways:\n\n- Taxi activity can now be reviewed against broad daily context variables.\n- Weather and holidays are clean citywide daily features.\n- Permitted events are represented as a coarse citywide daily signal (good enough for prototype).\n- Later zone-level work may benefit from more localized event features, but this table is sufficient for early context review.\n\nNext notebook (03_zone_level_aggregation.ipynb) will merge this daily context with zone-day taxi aggregates and compute the first friction score.')
    return

@app.cell
def _():
    from IPython.display import display

    import pandas as pd
    import numpy as np
    from pathlib import Path
    import matplotlib.pyplot as plt
    import seaborn as sns
    import holidays
    import urllib.parse  
    import requests

    pd.set_option("display.max_columns", 50)
    pd.set_option("display.float_format", "{:,.2f}".format)

    # Project paths
    from nyc_mobility_friction.paths import get_project_paths
    paths = get_project_paths()

    # Create external data folder if needed
    external_dir = paths.raw / "external"
    external_dir.mkdir(parents=True, exist_ok=True)

    print("External data folder ready:", external_dir)

    # Project paths
    from nyc_mobility_friction.paths import get_project_paths
    paths = get_project_paths()

    external_dir = paths.raw / "external"
    external_dir.mkdir(parents=True, exist_ok=True)

    print("External data folder ready:", external_dir)

    STUDY_START = pd.Timestamp("2025-01-01")
    STUDY_END   = pd.Timestamp("2025-04-01")   # exclusive

    print("Study window:", STUDY_START.date(), "to", (STUDY_END - pd.Timedelta(days=1)))

    taxi_dir = paths.raw / "taxi"
    taxi_files = sorted(taxi_dir.glob("yellow_tripdata_2025-*.parquet"))

    taxi = pd.concat([pd.read_parquet(f) for f in taxi_files], ignore_index=True)

    taxi = taxi.rename(columns=str.lower).rename(columns=lambda x: x.replace(" ", "_"))
    taxi["pickup_datetime"] = pd.to_datetime(taxi["tpep_pickup_datetime"], errors="coerce")
    taxi["dropoff_datetime"] = pd.to_datetime(taxi["tpep_dropoff_datetime"], errors="coerce")

    # Strict study window filter
    taxi = taxi[
        taxi["pickup_datetime"].notna() &
        (taxi["pickup_datetime"] >= STUDY_START) &
        (taxi["pickup_datetime"] < STUDY_END)
    ].copy()

    taxi["pickup_date"] = taxi["pickup_datetime"].dt.date

    print(f"Loaded {len(taxi):,} taxi trips after study-window filter")
    print("Date range:", taxi["pickup_datetime"].min(), "→", taxi["pickup_datetime"].max())

    zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    zones = pd.read_csv(zone_url)

    print(f"Loaded {len(zones)} taxi zones")
    display(zones.head())
    print("\nBorough distribution:")
    print(zones["Borough"].value_counts())

    print("Taxi rows after study-window filter:", len(taxi))
    print("Taxi date range:", taxi["pickup_datetime"].min(), "to", taxi["pickup_datetime"].max())

    events_path = external_dir / "nyc_permitted_events_jan_mar_2025.csv"

    try:
        from sodapy import Socrata
        print("Using official Socrata client...")
        client = Socrata("data.cityofnewyork.us", None)
    
        where_clause = f"start_date_time >= '2025-01-01' AND start_date_time <= '2025-03-31'"
    
        results = []
        offset = 0
        limit = 1000
        while True:
            batch = client.get("bkfu-528j", where=where_clause, limit=limit, offset=offset)
            if not batch:
                break
            results.extend(batch)
            offset += limit
            print(f"   Fetched {len(results):,} events...")
    
        events = pd.DataFrame.from_records(results)
        events.to_csv(events_path, index=False)
        print(f"✅ Loaded {len(events):,} events via sodapy")
    
    except Exception as e:
        print(f"⚠️ Socrata error: {e}")
        if events_path.exists():
            events = pd.read_csv(events_path)
            print(f"✅ Loaded cached file ({len(events):,} rows)")
        else:
            events = pd.DataFrame()

    # Clean events
    if len(events) > 0:
        events["start_date_time"] = pd.to_datetime(events["start_date_time"], errors="coerce")
        events["event_date"] = events["start_date_time"].dt.date
        daily_events = events.groupby("event_date").size().reset_index(name="citywide_permitted_event_count")
        print(f"Days with at least one permitted event: {len(daily_events)}")

    print("Permitted events rows:", len(events))
    print("Event date range:", events["start_date_time"].min(), "to", events["start_date_time"].max())

    def fetch_weather(start_date: str, end_date: str) -> pd.DataFrame:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": 40.78,
            "longitude": -73.97,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "daily": ",".join([
                "weather_code",
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "snowfall_sum",
                "precipitation_hours",
                "wind_speed_10m_max",
            ]),
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "precipitation_unit": "inch",
            "timezone": "America/New_York",
        }

        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()["daily"]

        weather = pd.DataFrame(data)
        weather["date"] = pd.to_datetime(weather["time"])
        weather = weather.drop(columns=["time"])
        weather["temp_avg_f"] = (
            weather["temperature_2m_max"] + weather["temperature_2m_min"]
        ) / 2
        weather["has_precip"] = weather["precipitation_sum"] > 0
        weather["has_snow"] = weather["snowfall_sum"] > 0
        return weather

    weather = fetch_weather(STUDY_START, STUDY_END)
    weather.head()

    print("Weather rows:", len(weather))
    print("Weather date range:", weather["date"].min(), "to", weather["date"].max())

    us_holidays = holidays.US(years=[2025], observed=True)

    holidays_df = pd.DataFrame({
        "date": pd.date_range(STUDY_START, STUDY_END, freq="D")
    })
    holidays_df["holiday_name"] = holidays_df["date"].map(lambda d: us_holidays.get(d.date()))
    holidays_df["is_holiday"] = holidays_df["holiday_name"].notna()

    holidays_df[holidays_df["is_holiday"]]

    print("Holiday rows:", len(holidays_df))
    print("Holiday dates:", holidays_df.loc[holidays_df["is_holiday"], "date"].dt.date.tolist())

    # Daily taxi volume (for context)
    daily_taxi = taxi.groupby("pickup_date").size().reset_index(name="trip_count")

    daily_events.event_date = pd.to_datetime(daily_events.event_date)
    holidays_df.date = pd.to_datetime(holidays_df.date)

    # Merge all context
    daily_context = daily_taxi.copy()
    daily_context = daily_context.merge(daily_events, left_on="pickup_date", right_on="event_date", how="left")
    daily_context = daily_context.merge(weather, left_on="pickup_date", right_on="date", how="left").drop(columns="date")
    daily_context = daily_context.merge(holidays_df, left_on="pickup_date", right_on="date", how="left").drop(columns="date")

    daily_context = daily_context.rename(columns={"pickup_date": "date"})
    daily_context["has_permitted_event_day"] = daily_context["citywide_permitted_event_count"].fillna(0) > 0
    daily_context["has_holiday"] = daily_context["holiday_name"].notna()
    daily_context = daily_context.fillna(0)

    print("Daily Context Preview (first 10 days):")
    display(daily_context.head(15))

    # Quick stats
    print(f"\nDays with precipitation: {(daily_context['has_precip']).sum()}")
    print(f"Days with snow: {(daily_context['has_snow']).sum()}")
    print(f"Days with major events: {daily_context['has_permitted_event_day'].sum()}")

    print("Plotting...")

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    ax1 = axes[0,0]
    daily_context.plot(x="date", y="trip_count", ax=ax1, label="Trips", color="blue")
    ax1.set_title("Daily Taxi Trips vs Events")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Trip Count")

    if "citywide_permitted_event_count" in daily_context.columns:
        ax1_twin = ax1.twinx()
        daily_context.plot(x="date", y="citywide_permitted_event_count", ax=ax1_twin, label="Events", color="red")
        ax1_twin.set_ylabel("Event Count")

    daily_context.plot(x="date", y="precipitation_sum", ax=axes[0,1], color="blue", title="Daily Precipitation (inches)")
    daily_context.plot(x="date", y="snowfall_sum", ax=axes[1,0], color="gray", title="Daily Snowfall (inches)")
    daily_context.plot(x="date", y="temp_avg_f", ax=axes[1,1], color="orange", title="Daily Avg Temperature (°F)")

    plt.tight_layout()
    plt.show()
    return


if __name__ == "__main__":
    app.run()
