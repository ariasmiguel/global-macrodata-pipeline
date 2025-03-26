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

## Development

- Main development happens in the `develop` branch
- Production code is in the `master` branch 