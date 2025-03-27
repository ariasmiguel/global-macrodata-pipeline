# Global Macrodata Pipeline

A data pipeline for processing and analyzing global macroeconomic data.

## Project Structure

```
global-macrodata-pipeline/
│
├── data/
│   ├── bronze/     # Raw data
│   ├── silver/     # Cleaned data
│   └── gold/       # Transformed data
│
├── scripts/
│   ├── extraction/ # Data extraction scripts
│   ├── cleaning/   # Data cleaning scripts
│   ├── transformation/ # Data transformation scripts
│   └── ingestion/  # Data ingestion scripts
│
├── dags/           # Airflow DAG files
├── tests/          # Unit and integration tests
└── docs/           # Documentation
```

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Unix/macOS
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Scripts Overview

The pipeline consists of several scripts that must be executed in sequence. Each script processes data from the previous stage and outputs to the next stage.

### Execution Order

1. Data Extraction (`scripts/`)
2. Data Validation (`scripts/`)
3. Data Processing (`scripts/`)

### Script Details

#### 1. Data Extraction
- **Purpose**: Fetches raw data from various sources
- **Location**: `scripts/`
- **Input**: None (external data sources)
- **Output**: Raw data files in `data/bronze/`
- **Key Files**:
  - `extract_bls_data.py`: Fetches economic indicators from BLS
  - `generate_ppi_series.py`: Generates PPI (Producer Price Index) series
  - `combine_ppi_series.py`: Combines multiple PPI series

#### 2. Data Validation
- **Purpose**: Validates raw data quality and completeness
- **Location**: `scripts/`
- **Input**: Files from `data/bronze/`
- **Output**: Validation reports and cleaned data in `data/silver/`
- **Key Files**:
  - `validate_bronze_data.py`: Validates raw data quality and structure

#### 3. Testing
- **Purpose**: Tests data series and transformations
- **Location**: `scripts/`
- **Input**: Data from various stages
- **Output**: Test results and reports
- **Key Files**:
  - `test_series.py`: Tests data series integrity and transformations

### Running the Pipeline

1. Start with data extraction:
```bash
python scripts/combine_ppi_series.py
python scripts/generate_ppi_series.py
python scripts/extract_bls_data.py
```

2. Validate the extracted data:
```bash
python scripts/validate_bronze_data.py
```

3. Run tests:
```bash
python scripts/test_series.py
```

## Development

- Main development happens in the `develop` branch
- Production code is in the `master` branch 