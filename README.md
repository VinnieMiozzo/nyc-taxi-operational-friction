# NYC Taxi Operational Friction

This project uses **NYC Yellow Taxi trip records** to identify taxi zones that show
signs of operational strain or inefficient service patterns.

The goal is not to explain all mobility issues in New York City. The goal is to support
a practical question:

**Which taxi zones should be prioritized for deeper operational review?**

## Decision context

City agencies and transportation operators cannot review every taxi zone at once.

This project is designed as a prioritization tool that flags zones where taxi activity
suggests possible operational friction, such as:

- unusually long trip times relative to distance
- persistent pickup/dropoff imbalance
- low pickup activity relative to surrounding demand patterns
- concentration of late-night or irregular service patterns
- repeated signs of weak zone-level performance over time

These zones may be candidates for:

- curb management review
- pickup/dropoff policy review
- taxi stand evaluation
- late-night service monitoring
- deeper operational investigation

## Current scope

The current phase uses **three months of taxi trip data** to validate the workflow, 
define useful indicators, and test whether taxi zones can support a stable zone-level 
prioritization framework.

At this stage, the project is a **prototype**, not a final policy recommendation.  
A longer time window will be needed before making stable conclusions about persistent 
zone-level patterns.

## Data source

- **NYC Taxi Trip Records**  
  Used to measure trip activity, trip outcomes, and zone-level operational patterns.

## Analytical approach

The project builds a zone-level view of taxi operations using indicators such as:

- pickup volume
- dropoff volume
- pickup/dropoff imbalance
- trip duration
- trip distance
- duration per mile
- late-night trip share
- invalid or extreme-trip rate
- day-level persistence of high-friction conditions

## Output

A prototype ranking of taxi zones where operational friction appears elevated and may 
justify deeper review.

## Repository structure

- `data/` raw and processed data
- `notebooks/` exploration and analysis
- `src/` reusable Python code
- `dashboard/` visualization app
- `report/` short memo and write-up

## Project status

In progress. Current work is focused on:

- data extraction and cleaning
- taxi-zone aggregation
- exploratory analysis
- feature design for zone-level scoring

## Limitations

- Three month of data is not enough to capture seasonality
- Taxi trips represent only part of urban travel demand
- Zone-level metrics may be sensitive to outliers and rare extreme trips
- Operational friction metrics are screening tools, not causal explanations

## Next steps

- refine taxi cleaning rules and outlier treatment
- build daily zone-level aggregates
- define a transparent friction score
- rank priority zones for review
- expand to multiple months
- develop a dashboard and short memo
