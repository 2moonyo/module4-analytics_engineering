# NYC Taxi Rides Data Analytics Engineering Project

This project processes and analyzes NYC Taxi & Limousine Commission (TLC) trip data for yellow taxis, green taxis, and For-Hire Vehicles (FHV) using dbt and DuckDB.

## Prerequisites

- Python 3.8+
- [uv](https://github.com/astral-sh/uv) package manager

## Project Location

**Important:** All dbt models and data for the local DuckDB setup are located in the `taxi_rides_ny/` folder. Navigate to this directory before running any commands:

```bash
cd taxi_rides_ny
```

## Setup Instructions

### 1. Install Dependencies

First, sync all project dependencies using `uv`:

```bash
uv sync
```

This will install all required Python packages and dbt dependencies.

### 2. Data Ingestion

#### Step 2.1: Ingest Yellow and Green Taxi Data

Run the main ingestion script to download and load yellow and green taxi data (2019-2020) into the DuckDB database:

```bash
uv run python ingest.py
```

This script will:
- Download yellow and green taxi trip data for 2019-2020
- Convert CSV.gz files to Parquet format
- Load data into `prod.yellow_tripdata` and `prod.green_tripdata` tables

#### Step 2.2: Ingest FHV (For-Hire Vehicle) Data

Load the FHV taxi data for 2019:

```bash
uv run python ingest_stg_fhv_ny.py --fhv --years "2019"
```

This will:
- Download FHV trip data for all months in 2019
- Convert to Parquet format
- Load data into `prod.fhv_tripdata` table

### 3. dbt Setup

#### Step 3.1: Install dbt Packages

Install required dbt packages (e.g., dbt-utils):

```bash
uv run dbt deps
```

#### Step 3.2: Load Seed Data

Load reference data from CSV files (taxi zones, payment types):

```bash
uv run dbt seed
```

### 4. Build and Run dbt Models

#### Step 4.1: Build All Models

Run the full dbt build process (models, tests, and seeds) targeting the production environment:

```bash
uv run dbt build --target prod
```

#### Step 4.2: Run Models

Alternatively, run just the dbt models:

```bash
uv run dbt run
```

## Project Structure

```
taxi_rides_ny/
├── analyses/           # Ad-hoc analysis queries
├── data/              # Downloaded parquet files (gitignored)
├── models/
│   ├── staging/       # Staging models (stg_*)
│   ├── intermediate/  # Intermediate transformations (int_*)
│   └── marts/         # Business layer models (dim_*, fct_*)
├── seeds/             # Reference data CSVs
└── tests/             # Custom data tests
```

## Analysis Queries

The `analyses/` folder contains SQL queries to answer specific questions about the data:

### Q3: Count of records in fct_monthly_zone_revenue

**File:** `analyses/fct_monthly_revenue_count.sql`

This query counts records in the monthly revenue per location fact table, showing any duplicates grouped by month, location, and taxi type.

### Q4: Zone with highest revenue for Green taxis in 2020

**File:** `analyses/zone_highest_green_zone_2020.sql`

Identifies which NYC taxi zone generated the highest revenue for green taxis during 2020.

### Q5: Total trips for Green taxis in October 2019

**File:** `analyses/total_trips_green_taxi_Oct2019.sql`

Calculates the total number of green taxi trips taken in October 2019.

### Q6: Count of records in stg_fhv_tripdata (filter dispatching_base_num IS NULL)

**File:** `analyses/count_fhv.sql`

Counts FHV trip records where the dispatching base number is NULL (should be 0 after staging filters are applied).

## Data Quality Notes

### Duplicate Handling Strategy

During development, the `QUALIFY` clause with `ROW_NUMBER()` window functions for deduplication caused Out-of-Memory (OOM) issues when processing the large taxi datasets. As a workaround:

1. **Primary deduplication approach**: Filter out the bulk of duplicates by removing trips with void/canceled status flags
2. **Remaining duplicates**: 2420 duplicate records remain in the dataset but are minimal compared to the total volume
3. **Test configuration**: Data quality tests for duplicates are configured to issue **warnings** rather than fail the build, allowing the pipeline to continue running
4. **Future optimization**: Full deduplication will be implemented when migrating to BigQuery, which has better memory management for large-scale window operations

This pragmatic approach balances data quality with resource constraints while maintaining transparency about known data issues.

## Running Specific Tests

To run only data tests:

```bash
uv run dbt test --target prod
```

## Resources

- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [DuckDB Documentation](https://duckdb.org/docs/)
