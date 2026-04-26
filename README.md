# Databricks ETL Pipeline (Bronze/Silver/Gold + Snowflake Schema)

This project builds a full ETL pipeline in Databricks using a large public CSV dataset (`nycflights13` flights data) that intentionally contains many null/NA values.

## What this pipeline does

1. Downloads raw CSV data from the web:
   - `flights.csv` (large, contains bad values / missing fields)
   - `airlines.csv`
   - `airports.csv`
2. Loads raw data into **bronze** Delta tables.
3. Cleans and types data into **silver** Delta tables.
4. Builds **gold** analytical model as a **snowflake schema**:
   - Fact: `fact_flights`
   - Dimensions: `dim_date`, `dim_month`, `dim_day_of_week`, `dim_carrier`, `dim_airport`, `dim_timezone`

## Project layout

- `scripts/install_cli.sh`: install Databricks CLI + Python libs
- `scripts/download_data.sh`: download CSV files
- `scripts/setup_databricks_cfg.sh`: create/update `~/.databrickscfg` from env vars
- `src/etl/01_bronze.py`: raw ingestion
- `src/etl/02_silver.py`: cleaning and standardization
- `src/etl/03_gold.py`: snowflake schema build
- `databricks.yml`: Databricks Asset Bundle job
- `conf/pipeline_config.yaml`: catalog/schema/raw path configuration
- `src/sql/validation_queries.sql`: sanity checks

## 1) Install CLI and libraries

```bash
bash scripts/install_cli.sh
```

## 2) Download CSV files

```bash
bash scripts/download_data.sh
```

## 3) Create Databricks config from PAT

```bash
cp .env.example .env
# edit .env and set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_PROFILE
source .env
bash scripts/setup_databricks_cfg.sh
```

This writes your profile to `~/.databrickscfg`, so future CLI commands can pull your PAT automatically.

## 4) Upload raw files to DBFS (example)

```bash
databricks fs cp data/raw/flights.csv dbfs:/FileStore/etl/raw/flights.csv --overwrite --profile <your-profile>
databricks fs cp data/raw/airlines.csv dbfs:/FileStore/etl/raw/airlines.csv --overwrite --profile <your-profile>
databricks fs cp data/raw/airports.csv dbfs:/FileStore/etl/raw/airports.csv --overwrite --profile <your-profile>
```

## 5) Configure bundle and run

1. Update `workspace.host` in `databricks.yml`.
2. Update catalog/schemas/paths in `conf/pipeline_config.yaml` if needed.
3. Validate/deploy/run:

```bash
databricks bundle validate --profile <your-profile>
databricks bundle deploy -t dev --profile <your-profile>
databricks bundle run flights_etl_job -t dev --profile <your-profile>
```

## Notes on data quality

The flights dataset includes missing values across delay/timing columns (`NA`, blank values, nulls), so silver cleaning handles:

- Null marker normalization
- Type casting
- Invalid date filtering
- Deduplication of flight-level keys
