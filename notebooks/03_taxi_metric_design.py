import marimo

__generated_with = "manual-conversion"
app = marimo.App()

@app.cell
def _():
    import marimo as mo
    return (mo,)

@app.cell(hide_code=True)
def _(mo):
    mo.md('# Notebook 03 — Taxi Metric Design\n\n## Purpose\n\nThis notebook is the first real analytical notebook in the project.\n\nThe goal is not to make a causal claim. The goal is to design a credible taxi-side monitoring layer that can support operational review.\n\nAt this stage, we focus only on taxi data and evaluate whether a pickup zone × day table can capture useful signals of mobility friction.\n\n## Analytical objectives\n\nThis notebook is used to:\n\n- verify that study-window filtering is enforced correctly\n- confirm that invalid trips are removed before grouped metrics are computed\n- inspect the distribution of key trip variables\n- construct robust daily zone-level taxi metrics\n- evaluate which metrics are stable enough to carry forward into later analysis\n\n## Important framing decisions\n\nCurrent project choices:\n\n- raw data remains untouched\n- cleaning happens before aggregation\n- the unit of analysis is pickup zone × day\n- low trip count alone does not define friction\n- early friction indicators should rely on robust statistics such as medians and upper-tail metrics rather than means\n\n## Known data-quality risks\n\nWe already know that raw TLC taxi files can contain timestamps outside the intended month or study window.\n\nBecause of that, the study window must be enforced explicitly using `pickup_datetime` before aggregation.\n\nWe also apply trip-level validity filters before computing zone-day metrics.\n\n---\n\n## Load taxi data\n\nFor this notebook, we should avoid loading raw multi-month trip data unless needed.\n\nThe preferred input is an already cleaned taxi table or a pre-aggregated daily zone table.\n\nIf a cleaned trip-level table is available, we can still use it here for metric design, but the notebook should stay scoped and memory-aware.')
    return

@app.cell
def _():
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt

    from nyc_mobility_friction.paths import get_project_paths

    paths = get_project_paths()

    pd.set_option("display.max_columns", 100)
    pd.set_option("display.max_rows", 200)

    taxi_path = paths.processed / "taxi" / "clean"
    taxi_files = sorted(taxi_path.rglob("*.parquet"))

    print(f"Found {len(taxi_files)} cleaned taxi files")

    taxi = pd.concat((pd.read_parquet(f) for f in taxi_files), ignore_index=True)

    print(taxi.shape)
    taxi.head()
    return


if __name__ == "__main__":
    app.run()
