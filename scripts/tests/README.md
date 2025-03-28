# Tests Directory

This directory contains test scripts for various components of the Macroeconomic Data Pipeline.

## Test Files

- `test_bls_series.py`: Tests BLS series ID validation and processing
- `test_connection.py`: Tests the connection to the ClickHouse database
- `test_db.py`: Tests database operations on ClickHouse
- `run_tests.py`: Script to run all tests and report results

## Running Tests

To run a specific test:

```bash
python scripts/tests/test_connection.py
```

To run all tests with the test runner:

```bash
python scripts/tests/run_tests.py
```

Alternatively, you can use this bash one-liner:

```bash
# Simple bash test runner
for test_file in scripts/tests/test_*.py; do
    echo "Running $test_file..."
    python "$test_file"
    echo "-----------------------"
done
```

## Adding New Tests

When adding new tests:

1. Name the file with a `test_` prefix
2. Include proper error handling and logging
3. Make sure tests can run independently
4. Add descriptive assertions that explain what failed

## Test Results

Test logs are saved to the `logs/tests/` directory with timestamps for review. 