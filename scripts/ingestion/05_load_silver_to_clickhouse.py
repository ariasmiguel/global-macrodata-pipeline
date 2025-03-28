"""
Script to load data from silver layer files into ClickHouse silver tables.
This is step 5 in the Macroeconomic Data Pipeline - loading cleaned data into the ClickHouse silver layer.
"""

import os
import logging
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import clickhouse_connect
import time
from typing import List, Dict, Any
import datetime
import uuid

# Configure logging
os.makedirs("logs/ingestion", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/ingestion/05_load_silver_to_clickhouse.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
BATCH_SIZE = 100000  # Number of rows to insert at once

def connect_to_clickhouse():
    """Connect to ClickHouse database."""
    try:
        # Get connection details from environment variables or use defaults
        host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
        port = 8123  # HTTP port hardcoded for simplicity
        username = os.environ.get('CLICKHOUSE_USER', 'default')
        password = os.environ.get('CLICKHOUSE_PASSWORD', 'clickhouse')
        database = os.environ.get('CLICKHOUSE_DB', 'macro')
        
        logger.info(f"Connecting to ClickHouse at {host}:{port}, database: {database}")
        
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database
        )
        
        # Test connection
        version = client.query("SELECT version()").first_row[0]
        logger.info(f"Successfully connected to ClickHouse version {version}")
        return client
    
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {str(e)}")
        raise

def load_series_metadata(silver_dir: Path, client) -> int:
    """Load series metadata from silver layer Parquet file to ClickHouse."""
    metadata_file = silver_dir / 'series_metadata.parquet'
    
    if not metadata_file.exists():
        logger.warning(f"Series metadata file not found: {metadata_file}")
        return 0
    
    try:
        logger.info(f"Loading series metadata from {metadata_file}")
        
        # Read the Parquet file
        df = pd.read_parquet(metadata_file)
        logger.info(f"Loaded {len(df)} series metadata records")
        
        # Convert string timestamps to datetime
        df['processed_timestamp'] = pd.to_datetime(df['processed_timestamp'])
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['updated_at'] = pd.to_datetime(df['updated_at'])
        
        # Convert series_id to UUID if it's not already
        if df['series_id'].dtype == 'object':
            try:
                # If series_id is not a valid UUID, generate new UUIDs
                df['series_id'] = df['series_id'].apply(lambda x: 
                    str(uuid.UUID(x)) if is_valid_uuid(x) else str(uuid.uuid4()))
            except:
                # Generate new UUIDs if conversion fails
                df['series_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        # Insert into ClickHouse
        client.query("TRUNCATE TABLE silver_series_metadata")
        client.insert_df('silver_series_metadata', df)
        
        logger.info(f"Inserted {len(df)} series metadata records into ClickHouse")
        return len(df)
    
    except Exception as e:
        logger.error(f"Error loading series metadata to ClickHouse: {str(e)}")
        return 0

def load_series_values(silver_dir: Path, client) -> int:
    """Load series values from silver layer Parquet files to ClickHouse."""
    values_dir = silver_dir / 'bls_data'
    
    if not values_dir.exists():
        logger.warning(f"Values directory not found: {values_dir}")
        return 0
    
    # Find the latest file using the timestamp pattern
    parquet_files = list(values_dir.glob('bls_data_*.parquet'))
    if not parquet_files:
        logger.warning(f"No BLS data files found in {values_dir}")
        return 0
    
    # Sort files by name (which includes timestamp) and get the latest
    latest_file = max(parquet_files, key=lambda x: x.name)
    logger.info(f"Found latest file: {latest_file}")
    
    try:
        # First truncate the table to avoid duplicates
        client.query("TRUNCATE TABLE silver_series_values")
        
        logger.info(f"Processing latest file: {latest_file}")
        
        # Read the Parquet file
        df = pd.read_parquet(latest_file)
        logger.info(f"Loaded {len(df)} records from {latest_file.name}")
        
        total_loaded = 0
        
        # Process in batches of 100,000 records
        batch_size = 100000
        for i in range(0, len(df), batch_size):
            try:
                batch = df.iloc[i:i+batch_size].copy()
                
                # Convert series_id to UUID if it's not already
                if batch['series_id'].dtype == 'object':
                    try:
                        # If series_id is not a valid UUID, generate new UUIDs
                        batch['series_id'] = batch['series_id'].apply(lambda x: 
                            str(uuid.UUID(x)) if is_valid_uuid(x) else str(uuid.uuid4()))
                    except:
                        # Generate new UUIDs if conversion fails
                        batch['series_id'] = [str(uuid.uuid4()) for _ in range(len(batch))]
                
                # Convert boolean values to integers
                batch['is_annual_avg'] = batch['is_annual_avg'].astype(int)
                
                # Convert timestamps to string format for ClickHouse
                batch['created_at'] = pd.to_datetime(batch['created_at'])
                batch['updated_at'] = pd.to_datetime(batch['updated_at'])
                batch['extraction_timestamp'] = pd.to_datetime(batch['extraction_timestamp'])
                
                # Convert date to string format
                batch['date'] = pd.to_datetime(batch['date'])
                
                # Insert into ClickHouse
                client.insert_df('silver_series_values', batch)
                
                total_loaded += len(batch)
                logger.info(f"Inserted batch of {len(batch)} records (total: {total_loaded})")
                
            except Exception as e:
                logger.error(f"Error processing batch: {str(e)}")
        
        logger.info(f"Completed loading series values ({total_loaded} records)")
        return total_loaded
    
    except Exception as e:
        logger.error(f"Error loading series values to ClickHouse: {str(e)}")
        return 0

# Helper function to check if a string is a valid UUID
def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except:
        return False

def main():
    """Main execution function."""
    start_time = time.time()
    
    # Define data directory
    silver_dir = Path("data/silver")
    
    if not silver_dir.exists():
        logger.error(f"Silver data directory not found: {silver_dir}")
        return
    
    logger.info(f"Starting silver layer data loading to ClickHouse")
    
    try:
        # Connect to ClickHouse
        client = connect_to_clickhouse()
        
        # Load series metadata
        metadata_count = load_series_metadata(silver_dir, client)
        
        # Load series values
        values_count = load_series_values(silver_dir, client)
        
        # Log summary
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info("====== Loading Summary ======")
        logger.info(f"Total series metadata records loaded: {metadata_count}")
        logger.info(f"Total series values loaded: {values_count}")
        logger.info(f"Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        logger.info("============================")
        
        # Save results to a summary file
        summary = {
            "timestamp": datetime.datetime.now().isoformat(),
            "metadata_records_loaded": metadata_count,
            "values_loaded": values_count,
            "execution_time_seconds": round(total_time, 2)
        }
        
        summary_dir = Path("data/logs")
        summary_dir.mkdir(parents=True, exist_ok=True)
        
        with open(summary_dir / "silver_loading_summary.json", "w") as f:
            import json
            json.dump(summary, f, indent=2)
        
    except Exception as e:
        logger.error(f"Error in loading process: {str(e)}")
    finally:
        logger.info("Silver layer loading process completed")

if __name__ == "__main__":
    main() 