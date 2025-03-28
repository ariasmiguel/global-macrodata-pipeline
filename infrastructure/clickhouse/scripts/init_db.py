"""
Script to initialize the ClickHouse database and tables.
Creates the macro database and required tables for economic indicators.
"""

import os
from pathlib import Path
from clickhouse_connect import get_client
from loguru import logger

# Configure logger
logger.add(
    "logs/infrastructure/init_db.log",
    rotation="500 MB",
    retention="10 days",
    level="INFO"
)

def init_database() -> None:
    """Initialize the ClickHouse database and tables."""
    client = get_client(
        host='localhost',
        port=8123,
        username='default',
        password='clickhouse'
    )
    
    try:
        # Create macro database
        logger.info("Creating macro database...")
        client.command('CREATE DATABASE IF NOT EXISTS macro')
        
        # Read schema file
        schema_path = Path(__file__).parent.parent / 'schemas' / '01_economic_indicators.sql'
        logger.info(f"Reading schema from {schema_path}")
        
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        
        # Execute each statement separately
        for statement in schema_sql.split(';'):
            if statement.strip():
                logger.info(f"Executing: {statement[:100]}...")
                client.command(statement)
        
        logger.info("Database initialization completed successfully!")
        
        # Verify tables were created
        tables = client.query('SHOW TABLES FROM macro')
        logger.info("\nCreated tables:")
        for table in tables.result_rows:
            logger.info(f"- {table[0]}")
            
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise
    finally:
        client.close()

if __name__ == "__main__":
    # Create logs directory if it doesn't exist
    os.makedirs("logs/infrastructure", exist_ok=True)
    init_database() 