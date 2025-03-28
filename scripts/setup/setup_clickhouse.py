#!/usr/bin/env python
"""Script to initialize ClickHouse database with schema files.

This script will:
1. Connect to ClickHouse
2. Execute all SQL schema files in order
3. Verify tables were created successfully
"""

import os
import sys
from pathlib import Path
import logging
import time
import subprocess
from typing import List, Dict, Any

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from macrodata_pipeline.database.clickhouse import get_clickhouse_client

# Set up logging
log_dir = project_root / "logs" / "setup"
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / "setup_clickhouse.log")
    ]
)
logger = logging.getLogger(__name__)

def get_schema_files() -> List[Path]:
    """Get all SQL schema files in order."""
    schema_dir = project_root / "infrastructure" / "clickhouse" / "schemas"
    
    if not schema_dir.exists():
        raise FileNotFoundError(f"Schema directory not found: {schema_dir}")
    
    # Get all .sql files and sort them by numeric prefix
    schema_files = [f for f in schema_dir.glob("*.sql") if f.name[0].isdigit()]
    schema_files.sort(key=lambda x: int(x.name.split("_")[0]))
    
    logger.info(f"Found {len(schema_files)} schema files:")
    for file in schema_files:
        logger.info(f"  - {file.name}")
    
    return schema_files

def execute_schema_file(client: Any, file_path: Path) -> None:
    """Execute a schema file."""
    logger.info(f"Executing schema file: {file_path.name}")
    
    try:
        # Read the schema file
        with open(file_path, 'r') as f:
            schema = f.read()
        
        # Split into individual statements
        statements = [stmt.strip() for stmt in schema.split(';') if stmt.strip()]
        
        # First, drop any existing tables/views
        drop_statements = []
        for stmt in statements:
            if 'CREATE TABLE' in stmt.upper():
                # Extract table name from CREATE TABLE statement
                table_name = stmt.split('(')[0].split()[-1]
                drop_statements.append(f"DROP TABLE IF EXISTS macro.{table_name};")
            elif 'CREATE MATERIALIZED VIEW' in stmt.upper():
                # Extract view name from CREATE MATERIALIZED VIEW statement
                view_name = stmt.split('TO')[0].split()[-1]
                drop_statements.append(f"DROP VIEW IF EXISTS macro.{view_name};")
        
        # Execute drop statements first
        for stmt in drop_statements:
            try:
                logger.debug(f"Executing drop statement:\n{stmt}")
                client.execute_query(stmt)
                logger.debug("Drop statement executed successfully")
            except Exception as e:
                logger.warning(f"Error executing drop statement:\n{stmt}\nError: {str(e)}")
        
        # Then execute create statements
        for stmt in statements:
            try:
                logger.debug(f"Executing statement:\n{stmt}")
                client.execute_query(stmt)
                logger.debug("Statement executed successfully")
            except Exception as e:
                logger.error(f"Error executing statement:\n{stmt}\nError: {str(e)}")
                raise
        
        logger.info(f"Successfully executed {file_path.name}")
        
    except Exception as e:
        logger.error(f"Error executing schema file {file_path.name}: {str(e)}")
        raise

def verify_tables(client: Any) -> Dict[str, bool]:
    """Verify that all required tables were created."""
    required_tables = [
        'bronze_data_sources',
        'bronze_series_metadata',
        'bronze_series_values',
        'silver_series_metadata',
        'silver_series_values',
        'gold_economic_indicators_metadata',
        'gold_economic_indicators_values',
        'gold_economic_indicators_monthly',
        'gold_economic_indicators_changes',
        'gold_economic_indicators_yearly'
    ]
    
    results = {}
    
    for table in required_tables:
        try:
            exists = client.execute_query(f"""
                SELECT 1 
                FROM system.tables 
                WHERE database = 'macro' 
                  AND name = '{table}'
            """)
            results[table] = len(exists) > 0
            logger.info(f"Table {table}: {'✓' if results[table] else '✗'}")
        except Exception as e:
            logger.error(f"Error checking table {table}: {str(e)}")
            results[table] = False
    
    return results

def main():
    """Initialize ClickHouse database with schema files."""
    start_time = time.time()
    logger.info("Starting ClickHouse database initialization")
    
    try:
        # Get schema files
        schema_files = get_schema_files()
        
        # Connect to ClickHouse
        with get_clickhouse_client() as client:
            # Execute each schema file
            for file in schema_files:
                execute_schema_file(client, file)
            
            # Verify tables
            logger.info("\nVerifying tables:")
            results = verify_tables(client)
            
            # Check if any tables are missing
            missing_tables = [table for table, exists in results.items() if not exists]
            
            if missing_tables:
                logger.error(f"Missing tables: {', '.join(missing_tables)}")
                sys.exit(1)
            else:
                logger.info("All tables created successfully!")
        
        duration = time.time() - start_time
        logger.info(f"\nDatabase initialization completed in {duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 