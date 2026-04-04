# NYC Mobility Friction

This project combines **NYC taxi trip records** and **NYC 311 service requests** to identify areas where resident-reported mobility problems and weaker taxi-service outcomes overlap.

The goal is not to explain all mobility issues in New York City. The goal is to support a more practical question:

**Which taxi zones should be prioritized for deeper operational review or intervention?**

## Decision context

Local government cannot investigate every street-level mobility issue at once.  
This project is designed as a prioritization tool that flags areas where:

- taxi activity is operationally strained, and
- residents repeatedly report mobility-related problems

These areas may be candidates for:
- curb-management review
- pickup/dropoff enforcement
- taxi stand review
- late-night service monitoring
- further field investigation

## Current scope

The current phase uses **one month of data** from both sources to validate the workflow, test geography alignment, and identify useful indicators.

At this stage, the project is a **prototype**, not a final policy recommendation.  
A longer time window will be needed before making stable zone-level conclusions.

## Data sources

- **NYC Taxi Trip Records**  
  Used to measure taxi demand, trip patterns, and service outcomes.

- **NYC 311 Service Requests**  
  Used as a resident-reported signal of local mobility friction.

## Analytical approach

The project builds a zone-level view of mobility conditions by combining:

### Taxi metrics
- pickup and dropoff volume
- trip duration
- trip distance
- duration per mile
- late-night trip share

### 311 metrics
- mobility-related request count
- complaint type mix
- closure time
- repeat issue frequency

### Combined output
A prototype ranking of zones where taxi-service friction and resident-reported issues appear to overlap.

## Repository structure

- `data/` raw and processed data
- `notebooks/` exploration and analysis
- `src/` reusable Python code
- `dashboard/` visualization app
- `report/` policy memo and write-up

## Project status

In progress. Current work is focused on:
- data extraction and cleaning
- geography alignment
- early exploratory analysis
- feature design for zone-level scoring

## Limitations

- One month of data is not enough to capture seasonality
- 311 requests reflect reporting behavior as well as underlying conditions
- Taxi trips represent only part of city mobility
- Spatial matching may introduce measurement error

## Next steps

- expand to multiple months
- refine the mobility-related 311 subset
- build zone-level scoring features
- develop a dashboard and short policy memo
