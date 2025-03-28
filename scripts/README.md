# Scripts Directory

This directory contains all the scripts for the Macroeconomic Data Pipeline, organized by their function and execution order.

## Directory Structure

### Ingestion (Steps 1-3)
- `01_generate_ppi_series.py`: Generate list of PPI series to extract
- `02_extract_bls_data.py`: Extract data from BLS API
- `03_load_bronze_to_clickhouse.py`: Load raw data into ClickHouse bronze layer

### Cleaning (Steps 4-5)
- `04_transform_to_silver.py`: Transform bronze data into silver layer
- `05_transform_to_gold.py`: Transform silver data into gold layer

### Loading (Steps 6-7)
- `06_load_silver_to_clickhouse.py`: Load silver data into ClickHouse
- `07_load_gold_to_clickhouse.py`: Load gold data into ClickHouse

### Analytics (Steps 8-10)
- `08_generate_dashboard_data.py`: Generate data for visualization dashboards
- `09_generate_blog_post.py`: Generate automated blog posts
- `10_supply_chain_pressures.py`: Calculate supply chain pressure indicators

### Setup
- `setup_clickhouse.py`: Initialize ClickHouse database and schemas

### Validation
- `validate_bronze_data.py`: Validate raw data in bronze layer
- `validate_clickhouse_data.py`: Validate data in ClickHouse tables

## Pipeline Flow
1. Generate list of PPI series to extract
2. Extract data from BLS API
3. Load raw data into bronze layer
4. Transform to silver layer (cleaning & standardization)
5. Transform to gold layer (analytics-ready)
6. Load silver data into ClickHouse
7. Load gold data into ClickHouse
8. Generate dashboard data
9. Generate blog posts
10. Calculate supply chain indicators

## Usage
Run scripts in order using the main pipeline script:
```bash
python run_pipeline.py --start-step 1 --end-step 10
```

Or run individual scripts as needed:
```bash
python scripts/ingestion/01_generate_ppi_series.py
``` 