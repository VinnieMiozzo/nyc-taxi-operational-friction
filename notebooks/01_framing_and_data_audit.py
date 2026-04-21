import marimo

__generated_with = "0.23.2"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Framing and Feasibility Audit: NYC Taxi Operational Friction

    **Portfolio Project**
    **Author:** Vinicius Miozzo
    **Date:** April 2026

    ---

    ## Objective

    This project uses **NYC Yellow Taxi trip records** to identify taxi zones that show signs of **operational strain or inefficient service patterns**.

    The goal is **not** to explain all mobility issues in New York City.
    The goal is to answer a practical prioritization question:

    **Which taxi zones should be prioritized for deeper operational review?**

    ---

    ## Decision Context

    City agencies and transportation operators cannot review every taxi zone at once.

    This project acts as a **prioritization tool** that flags zones where taxi activity suggests possible operational friction, such as:

    - Unusually long trip times relative to distance
    - Persistent pickup/dropoff imbalance
    - Concentration of late-night or irregular service patterns
    - Repeated signs of weak zone-level performance over time

    ---

    ## Working Question

    **Which taxi zones should be prioritized for deeper operational review?**

    ---

    ## Current Scope

    - **Taxi data**: January, February, and March 2025 (all three months loaded together)
    - The three months are used to validate the workflow, **check data consistency across months**, define useful indicators, and confirm that taxi zones can support a stable zone-level prioritization framework.
    - At this stage, the project is a **prototype**, not a final policy recommendation.
    - A longer time window will be needed before making stable conclusions about persistent zone-level patterns.
    - Exploration of external enrichment (weather + events) to improve interpretability

    ---

    ## Data Source

    - **NYC Taxi Trip Records** (Yellow Taxi)
      Used to measure trip activity, trip outcomes, and zone-level operational patterns.

    ---

    ## Analytical Approach

    The project builds a **zone-level view** of taxi operations using indicators such as:

    - Pickup volume
    - Dropoff volume
    - Pickup/dropoff imbalance
    - Trip duration
    - Trip distance
    - Duration per mile
    - Late-night trip share
    - Invalid or extreme-trip rate
    - Day-level persistence of high-friction conditions

    ---

    ## Proposed Unit of Analysis

    **Taxi Zone × Day**

    ---

    ## Candidate Metrics (Taxi-side)

    Per taxi zone per day:
    - Pickup trip count
    - Dropoff trip count
    - Pickup/dropoff imbalance ratio
    - Average / median trip duration
    - Duration per mile
    - Late-night trip share (10 pm – 6 am)
    - Invalid trip rate (duration ≤ 0 or distance ≤ 0)

    ---

    ## Repository Structure

    - `data/` — raw and processed data
    - `notebooks/` — exploration and analysis
    - `src/` — reusable Python code
    - `dashboard/` — visualization app
    - `report/` — short memo and write-up

    ---

    ## Project Status

    **In progress.**
    Current work is focused on data extraction, cleaning, and consistency checks across the first three months.

    ---

    ## Limitations

    - Three months of data give a first look at consistency but are not enough to capture full seasonality.
    - Taxi trips represent only part of urban travel demand.
    - Operational friction metrics are **screening tools**, not causal explanations.

    ---

    ## Next Steps

    1. Refine taxi cleaning rules and outlier treatment
    2. Build daily zone-level aggregates
    3. Define a transparent friction score
    4. Rank priority zones for review
    5. Expand to more months
    6. Develop a dashboard and short memo

    ---

    ## 1. Setup & Imports

    ---

    ## 2. Load Data (January + February + March 2025)

    ---

    ## 3. Data Audit

    ---

    ## 4. Taxi Cleaning Rules & Friction Metrics

    ---

    ## 5. Opportunities for External Data Enrichment (New)

    Adding contextual data will make friction metrics far more actionable and defensible.

    **Recommended enrichments (all feasible for prototype):**

    - **Daily weather** (temperature, precipitation, snow, wind)
      → Explains spikes in trip duration and low pickup activity on bad-weather days.
      → Sources: NOAA, Weather Spark, Open-Meteo Historical API, or Kaggle “Daily Weather Data for Major Cities (Jan–Feb 2025)”.

    - **Major events & holidays**
      → Flags days with parades, concerts, sports events, or holidays that distort normal patterns.
      → Source: NYC Open Data – “NYC Permitted Event Information – Historical” (CSV, covers 2025).

    - **Simple flags** (easy to add later)
      - Is holiday
      - Precipitation > 0.1 in
      - Snow > 0 in
      - Has major event

    These can be joined at the **day level** with almost zero extra code and will let you create an “adjusted friction score” that controls for external factors.

    ---

    ## 6. Key Takeaways (Feasibility Audit Complete)

    This framing and feasibility audit shows that NYC Yellow Taxi trip records are suitable for building a prototype zone-level operational friction prioritization tool.

    Key outcomes of this notebook:

    - The audit detected a small number of out-of-window timestamp records from the import process
    - Month-to-month trip volume and valid-trip rates are broadly consistent across the target three-month window
    - Core fields needed for Taxi Zone × Day aggregation are available and usable
    - Additional outlier treatment is still needed before final friction scoring
    - `PULocationID` remains a stable and practical geographic unit for aggregation
    - The candidate metrics described in the README are directly computable from the raw trip data

    Within its current prototype scope, the project appears feasible. The chosen unit of analysis (Taxi Zone × Day) provides a practical balance between granularity and stability.
    """)
    return


@app.cell
def _():
    from IPython.display import display

    import pandas as pd
    import numpy as np
    from pathlib import Path
    import matplotlib.pyplot as plt
    import seaborn as sns

    pd.set_option("display.max_columns", 120)
    pd.set_option("display.max_rows", 100)
    pd.set_option("display.width", 140)
    pd.set_option("display.float_format", "{:,.2f}".format)

    # Project paths
    from nyc_mobility_friction.paths import get_project_paths
    paths = get_project_paths()

    taxi_dir = paths.raw / "taxi"

    # Discover all three months
    taxi_files = sorted(taxi_dir.glob("yellow_tripdata_2025-*.parquet"))
    print("Available taxi files:")
    for f in taxi_files:
        print(f"   • {f.name}")

    # Load all three months together
    taxi = pd.concat([pd.read_parquet(f) for f in taxi_files], ignore_index=True)

    print(f"\nTotal taxi trips (Jan + Feb + Mar 2025): {len(taxi):,}")

    # Normalize column names
    taxi = taxi.rename(columns=str.lower).rename(columns=lambda x: x.replace(" ", "_"))

    # Safe datetime parsing
    for old, new in [("tpep_pickup_datetime", "pickup_datetime"),
                     ("tpep_dropoff_datetime", "dropoff_datetime")]:
        if old in taxi.columns:
            taxi[new] = pd.to_datetime(taxi[old], errors="coerce")

    taxi["pickup_month"] = taxi["pickup_datetime"].dt.to_period("M").astype(str)

    # Quick consistency overview
    monthly_summary = taxi.groupby("pickup_month").agg(
        total_trips=("pickup_datetime", "size"),
        unique_days=("pickup_datetime", lambda x: x.dt.date.nunique()),
        valid_trips_pct=("trip_distance", lambda x: (x > 0).mean() * 100)  # rough first look
    ).round(2)

    print("Monthly Data Consistency Overview:")
    display(monthly_summary)

    def audit_df(df: pd.DataFrame, name: str) -> pd.DataFrame:
        summary = pd.DataFrame({
            "column": df.columns,
            "dtype": df.dtypes.astype(str).values,
            "missing_pct": (df.isna().mean() * 100).round(3),
            "n_unique": [df[col].nunique(dropna=True) if df[col].dtype != "object" 
                         else df[col].astype(str).nunique(dropna=True) 
                         for col in df.columns]
        }).sort_values(["missing_pct", "n_unique"], ascending=[False, False])
        print(f"\n{name.upper()} AUDIT")
        print("-" * (len(name) + 6))
        print(f"Shape: {df.shape}")
        return summary

    taxi_audit = audit_df(taxi, "Taxi (3 Months)")
    display(taxi_audit.head(15))

    taxi["trip_duration_min"] = (
        (taxi["dropoff_datetime"] - taxi["pickup_datetime"]).dt.total_seconds() / 60
    )

    taxi["valid_trip"] = (
        (taxi["trip_duration_min"] > 0) &
        (taxi["trip_distance"] > 0) &
        (taxi["fare_amount"] > 0)
    )

    print(f"Overall valid trips (3 months): {taxi['valid_trip'].sum():,} / {len(taxi):,} "
          f"({taxi['valid_trip'].mean():.1%})")

    # Per-month valid trip rate
    monthly_valid = taxi.groupby("pickup_month")["valid_trip"].mean().mul(100).round(1).astype(str) + "%"
    print("\nValid trip percentage by month:")
    print(monthly_valid)

    numeric_cols = ["trip_distance", "fare_amount", "total_amount", 
                    "tip_amount", "trip_duration_min"]
    display(taxi[taxi["valid_trip"]][numeric_cols].describe(percentiles=[0.01, 0.05, 0.5, 0.95, 0.99]))
    return


if __name__ == "__main__":
    app.run()
