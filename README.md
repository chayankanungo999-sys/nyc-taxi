
## How to run
Import all files from`Notebooks/` into one Databricks folder, open **`run_pipeline`** and click **Run all**. It runs:
`01_landing` (check files exist on the TLC site, download) → `02_bronze` (load files as published) →
`03_combined_trips` (clean, combine, join zones, quality checks) → `04_analysis` (the 3 questions in SQL, saved as CSV).
Settings are plain variables in `00_config` (`taxi_types`, `year_months`, schema `nyc_taxi`); all paths and table names are
built from them. 


## Data quality issues and how I handled them
| Issue found | Handling |
|---|---|
| Pickup outside the file's month | Removed & Quarantined |
| Dropoff before pickup (223) | Removed & Quarantined  |
| Dropoff equal to pickup (~3,100) | Kept – paid trips with a wrong dropoff time |
| Negative `total_amount` (~134k): refunds of earlier charges | Kept, flagged `is_refund`; not counted as trips, included in revenue |
| Negative `fare_amount` (a meter fare can't be negative) | Value set to empty, trip kept |
| Impossible distances (up to 328,828 miles in minutes) | Over 100 miles **and** over 100 mph → distance set to empty, trip kept. |
| Empty rate code / payment type | Replaced with TLC's "unknown" codes 99 / 5,  as per  data dictionary |
| Zone names repeat for different ids | Busiest zones grouped by zone id; run stops if the lookup has duplicate ids |
| Duplicate rows | None found |



## Yellow vs green columns
Pickup/dropoff times are renamed from `tpep_` / `lpep_` to one name, then combined with `unionByName(allowMissingColumns=True)`.
Columns only one type has are empty for the other (`airport_fee` yellow, `trip_type` green; `cbd_congestion_fee` starts Jan 2025).
`ehail_fee` (always empty) is dropped. `combined_trips` has a fixed schema, so every column has a defined type.


## Assumptions
Revenue = sum of `total_amount` (what passengers paid, excl. cash tips), net of refunds. Negative `total_amount` represents refunds or financial adjustment, without a trip ID they can't be matched to their original trip.

## Logging and how I'd know it broke
Each stage prints what it did: files available and downloaded, rows loaded per file, rows loaded vs quarantined, unmapped zones,
PASS / WARNING / FAIL per check. Bad settings, a missing source file, an empty load, duplicate zone ids or a failed check
raise an error and stop the run. Unusual row counts reveal silent problems.

## With more time
schedule as a databricks job with failure handling & alerts · download to a temp folder and validate before replacing files · keep
older file versions  · store results per run 
