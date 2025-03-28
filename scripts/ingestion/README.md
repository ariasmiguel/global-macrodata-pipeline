# Data Ingestion Scripts

This directory contains scripts responsible for ingesting data from various sources into our data pipeline.

## Scripts Overview

### BLS Data Ingestion
- `extract_bls_data.py`: Extracts raw data from the BLS API
- `generate_ppi_series.py`: Generates PPI series data from raw BLS data
- `combine_ppi_series.py`: Combines multiple PPI series into a unified dataset

### Silver to Gold Layer Ingestion
- `load_silver_to_clickhouse.py`: Loads processed data from silver layer (Parquet files) into ClickHouse (gold layer)

## Data Flow

1. **Bronze Layer Ingestion**
   - Raw data is extracted from BLS API
   - Data is stored in Parquet format in the bronze layer

2. **Silver Layer Processing**
   - Raw data is cleaned and transformed
   - Series are combined and standardized
   - Data is stored in Parquet format in the silver layer

3. **Gold Layer Ingestion**
   - Processed data is loaded into ClickHouse
   - Data is organized in a star schema:
     - `economic_indicators_metadata`: Dimension table with series information
     - `economic_indicators_values`: Fact table with time series data
   - Materialized views are created for common aggregations

## Usage

1. Extract raw data:
```bash
python extract_bls_data.py
```

2. Generate PPI series:
```bash
python generate_ppi_series.py
```

3. Combine series:
```bash
python combine_ppi_series.py
```

4. Load to ClickHouse:
```bash
python load_silver_to_clickhouse.py
```

## Dependencies
- pandas
- clickhouse-connect
- loguru
- pathlib
- uuid 