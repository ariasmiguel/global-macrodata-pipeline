# Global Macrodata Pipeline

A data pipeline for processing and analyzing global macroeconomic data, with a focus on Bureau of Labor Statistics (BLS) data.

## Project Structure

```
global-macrodata-pipeline/
│
├── data/
│   ├── bronze/     # Raw data from sources
│   ├── silver/     # Cleaned and validated data
│   └── gold/       # Transformed and aggregated data
│
├── src/
│   └── macrodata_pipeline/
│       ├── extractors/     # Data extraction modules
│       │   ├── base.py     # Base extractor class
│       │   └── bls.py      # BLS data extractor
│       ├── transformers/   # Data transformation modules
│       └── loaders/        # Data loading modules
│
├── scripts/
│   ├── download_ppi_codes.py    # Download PPI industry and product codes
│   ├── generate_ppi_series.py   # Generate and validate PPI series IDs
│   ├── extract_bls_series.py    # Extract BLS series information
│   ├── test_series.py          # Test BLS series validity
│   └── extract_bls_data.py     # Extract BLS time series data
│
├── dags/           # Airflow DAG files
├── tests/          # Unit and integration tests
└── docs/           # Documentation
```

## Features

- Automated extraction of BLS data using their public API
- Support for multiple BLS surveys (PPI, CPI, CES, etc.)
- Rate limiting and API request tracking
- Data validation and cleaning
- Modular and extensible architecture

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

3. Set up environment variables:
```bash
export BLS_API_KEY=your_api_key_here
```

## Usage

1. Download PPI codes:
```bash
python scripts/download_ppi_codes.py
```

2. Generate and validate PPI series:
```bash
python scripts/generate_ppi_series.py
```

3. Extract BLS series information:
```bash
python scripts/extract_bls_series.py
```

4. Test specific series:
```bash
python scripts/test_series.py
```

5. Extract BLS data:
```bash
python scripts/extract_bls_data.py
```

## Development

- Main development happens in the `develop` branch
- Production code is in the `main` branch
- Follow PEP 8 style guidelines
- Write tests for new features
- Update documentation as needed

## Contributing

1. Create a feature branch from `develop`
2. Make your changes
3. Write/update tests
4. Submit a pull request to `develop`

## License

MIT License 