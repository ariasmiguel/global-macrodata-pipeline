# Infrastructure Directory

This directory contains infrastructure configurations and setup scripts for the Macroeconomic Data Pipeline.

## Directory Structure

- `clickhouse/`: ClickHouse database configurations and setup scripts
  - `schemas/`: SQL schema definitions for database tables
    - `01_init.sql`: Initial database creation
    - `02_bronze_layer.sql`: Bronze layer table schemas
    - `03_silver_layer.sql`: Silver layer table schemas
    - `04_gold_layer.sql`: Gold layer table schemas including materialized views
  - `docker/`: Docker configuration for running ClickHouse locally
  - `deploy/`: Deployment configurations
  - `scripts/`: Utility scripts for ClickHouse operations

## Setup Instructions

### Local Development with Docker

1. Navigate to the docker directory:
```bash
cd infrastructure/clickhouse/docker
```

2. Start ClickHouse with Docker Compose:
```bash
docker-compose up -d
```

3. Initialize the database schema:
```bash
python scripts/setup/setup_clickhouse.py
```

### Production Deployment

For production deployment, see the configurations in the `infrastructure/clickhouse/deploy/` directory. 