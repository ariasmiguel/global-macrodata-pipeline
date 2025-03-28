"""
Script to drop and recreate ClickHouse tables for the macroeconomic data pipeline.
"""

import os
from clickhouse_connect import get_client
from loguru import logger
from pathlib import Path

# Configure logger
logger.add(
    "logs/clickhouse/recreate_tables.log",
    rotation="500 MB",
    retention="10 days",
    level="INFO"
)

def recreate_tables() -> None:
    """Drop and recreate all tables in the macro database."""
    # Initialize ClickHouse client
    client = get_client(
        host='localhost',
        port=8123,
        username='default',
        password='clickhouse'
    )
    
    try:
        # Drop existing tables and views
        logger.info("Dropping existing tables and views...")
        drop_queries = [
            "DROP TABLE IF EXISTS macro.economic_indicators_yearly",
            "DROP TABLE IF EXISTS macro.economic_indicators_changes",
            "DROP TABLE IF EXISTS macro.economic_indicators_monthly",
            "DROP TABLE IF EXISTS macro.economic_indicators_values",
            "DROP TABLE IF EXISTS macro.economic_indicators_metadata"
        ]
        
        for query in drop_queries:
            client.command(query)
            logger.info(f"Executed: {query}")
        
        # Read and execute schema file
        schema_file = Path(__file__).parent.parent / "schemas" / "01_economic_indicators.sql"
        logger.info(f"Reading schema from {schema_file}")
        
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        # Split into individual statements and execute
        statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
        
        for stmt in statements:
            client.command(stmt)
            logger.info(f"Executed: {stmt[:100]}...")
        
        logger.info("Successfully recreated all tables and views")
        
    except Exception as e:
        logger.error(f"Error recreating tables: {str(e)}")
        raise
    finally:
        client.close()

if __name__ == "__main__":
    # Create logs directory if it doesn't exist
    os.makedirs("logs/clickhouse", exist_ok=True)
    recreate_tables() 