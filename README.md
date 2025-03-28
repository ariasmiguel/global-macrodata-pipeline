# Global Macrodata Pipeline

A comprehensive data pipeline for ingesting, processing, and analyzing macroeconomic data from the U.S. Bureau of Labor Statistics (BLS) API.

## Project Overview

This project implements a data pipeline using the Medallion Architecture (Bronze, Silver, Gold layers) to process macroeconomic data from the BLS API. The pipeline leverages Python, ClickHouse, and other tools to create analytics-ready datasets for investment analytics, dashboards, and blog generation.

## Directory Structure

```
global-macrodata-pipeline/
├── data/                      # Data storage
│   ├── bronze/                # Raw data (Bronze layer)
│   ├── silver/                # Cleaned data (Silver layer)
│   ├── gold/                  # Analytics-ready data (Gold layer)
│   ├── dashboards/            # Dashboard-ready datasets
│   ├── blog_posts/            # Generated economic analysis blog posts
│   └── reports/               # Generated data reports
├── infrastructure/            # Infrastructure as code
│   └── clickhouse/            # ClickHouse database setup
│       └── schemas/           # Database schema files
│           ├── 01_init.sql             # Initial database setup
│           ├── 02_bronze_layer.sql     # Bronze layer schema
│           ├── 03_silver_layer.sql     # Silver layer schema
│           └── 04_gold_layer.sql       # Gold layer schema
├── logs/                      # Application logs
├── scripts/                   # Processing scripts
│   ├── ingestion/             # Data ingestion scripts
│   │   ├── 01_generate_ppi_series.py    # Step 1: Generate PPI series IDs
│   │   ├── 02_extract_bls_data.py       # Step 2: Extract BLS API data
│   │   ├── 03_load_bronze_to_clickhouse.py # Step 3: Load data to Bronze layer
│   │   └── 05_load_silver_to_clickhouse.py # Step 5: Load data to Silver layer
│   ├── cleaning/              # Data cleaning scripts
│   │   └── 04_clean_bls_data.py         # Step 4: Clean and transform data
│   ├── analytics/             # Analytics scripts
│   │   ├── 06_transform_to_gold.py      # Step 6: Transform to Gold layer
│   │   ├── 07_load_gold_to_clickhouse.py # Step 7: Load and validate Gold layer
│   │   └── 08_generate_dashboard_data.py # Step 8: Generate dashboard datasets
│   └── run_pipeline.py        # Main pipeline orchestration script
└── src/                       # Source code modules
    └── macrodata_pipeline/    # Main package
        ├── extractors/        # Data extraction modules
        ├── transformers/      # Data transformation modules
        └── utils/             # Utility modules
```

## Pipeline Steps

The data pipeline consists of the following steps:

1. **Initialize Database Schema**: Set up ClickHouse database with the necessary tables and schemas
2. **Extract Data**: Fetch macroeconomic data from the BLS API
3. **Bronze Layer**: Load raw data into ClickHouse bronze tables
4. **Clean Data**: Process and clean raw data for further analysis
5. **Silver Layer**: Load cleaned data into ClickHouse silver tables
6. **Transform to Gold**: Create analytics-ready datasets in the gold layer
7. **Validate Gold Layer**: Verify and document gold layer datasets
8. **Generate Dashboard Data**: Create data for visualization dashboards and reports
9. **Generate Blog Posts**: Create economic analysis blog posts based on the latest indicators

## Running the Pipeline

You can run the complete pipeline or individual steps:

```bash
# Run the complete pipeline
python scripts/run_pipeline.py

# Run specific steps
python scripts/run_pipeline.py --start-step 3 --end-step 5

# Run with debug logging
python scripts/run_pipeline.py --debug
```

Or run individual scripts directly:

```bash
# Run step 1 - Generate PPI series IDs
python scripts/ingestion/01_generate_ppi_series.py

# Run step 2 - Extract data from BLS API
python scripts/ingestion/02_extract_bls_data.py
```

## Data Layers

The pipeline follows the Medallion Architecture with three distinct layers:

### Bronze Layer (Raw Data)
- Raw data from the BLS API
- Minimal transformations
- Preserved in original format with extraction metadata

### Silver Layer (Cleaned Data)
- Structured, cleaned data
- Validated and standardized
- Type-converted and deduplicated

### Gold Layer (Analytics-Ready)
- Business-ready datasets
- Aggregated for analytics
- Includes materialized views for different time periods
- Performance-optimized for dashboards and reporting

## ClickHouse Schema

The project uses ClickHouse for data storage with the following table structure:

1. **Bronze Layer**
   - `bronze_series_metadata` - Metadata for each series
   - `bronze_series_values` - Raw time series values

2. **Silver Layer**
   - `silver_series_metadata` - Cleaned series metadata
   - `silver_series_values` - Cleaned time series values

3. **Gold Layer**
   - `gold_economic_indicators_metadata` - Finalized indicator metadata
   - `gold_economic_indicators_values` - Core indicator values
   - `gold_economic_indicators_monthly` - Monthly aggregated view
   - `gold_economic_indicators_yearly` - Yearly aggregated view
   - `gold_economic_indicators_changes` - Month-over-month and year-over-year changes

## Configuration

To use the BLS API, you need to configure your API keys:

1. Create a file at `~/.bls_api_keys.json` with the following format:
```json
{
  "keys": ["YOUR_API_KEY_1", "YOUR_API_KEY_2"]
}
```

2. Set environment variables for ClickHouse connection:
```bash
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=
export CLICKHOUSE_DB=macro
```

## Setup and Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/global-macrodata-pipeline.git
cd global-macrodata-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Initialize ClickHouse database:
```bash
clickhouse-client -m < infrastructure/clickhouse/schemas/01_init.sql
clickhouse-client -m < infrastructure/clickhouse/schemas/02_bronze_layer.sql
clickhouse-client -m < infrastructure/clickhouse/schemas/03_silver_layer.sql
clickhouse-client -m < infrastructure/clickhouse/schemas/04_gold_layer.sql
```

## Dependencies

- Python 3.10+
- ClickHouse
- pandas
- pyarrow
- clickhouse-connect
- psutil 