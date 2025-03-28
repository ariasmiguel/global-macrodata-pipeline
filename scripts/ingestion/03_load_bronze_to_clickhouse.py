#!/usr/bin/env python
"""Script to load raw data into ClickHouse bronze layer.

This is step 3 in the Macroeconomic Data Pipeline - loading raw data into the ClickHouse bronze layer.
"""

import os
import sys
from pathlib import Path
import logging
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any
from dotenv import load_dotenv

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

# Load environment variables
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

from macrodata_pipeline.utils import get_logger
from macrodata_pipeline.database.clickhouse import get_clickhouse_client

# Set up logging
log_dir = project_root / "logs" / "ingestion"
log_dir.mkdir(parents=True, exist_ok=True)
logger = get_logger(
    __name__,
    log_file=log_dir / "03_load_bronze_to_clickhouse.log"
)

# Debug: Print environment variables
logger.info("Connection parameters:")
logger.info(f"CLICKHOUSE_HOST: {os.environ.get('CLICKHOUSE_HOST')}")
logger.info(f"CLICKHOUSE_PORT: {os.environ.get('CLICKHOUSE_PORT')}")
logger.info(f"CLICKHOUSE_USER: {os.environ.get('CLICKHOUSE_USER')}")
logger.info(f"CLICKHOUSE_DB: {os.environ.get('CLICKHOUSE_DB')}")

def load_series_metadata(bronze_dir: Path, client) -> int:
    """Load series metadata from bronze layer JSON file to ClickHouse."""
    metadata_file = bronze_dir / 'raw_series_data' / 'consolidated_data.json'
    
    if not metadata_file.exists():
        logger.warning(f"Series metadata file not found: {metadata_file}")
        return 0
    
    try:
        start_time = datetime.now()
        logger.info(f"Loading series metadata from {metadata_file}")
        
        # Read the JSON file
        with open(metadata_file, 'r') as f:
            data = json.load(f)
        
        # Extract series metadata
        series_metadata = []
        current_time = datetime.now()
        
        for series_id, series_data in data.items():
            metadata = {
                'series_id': series_id,
                'series_name': series_data.get('series_name', ''),
                'series_type': series_data.get('series_type', ''),
                'is_valid': 1,  # Default to valid
                'raw_data': json.dumps(series_data),  # Store complete raw data as JSON
                'raw_metadata': json.dumps({
                    'source': series_data.get('source', {}),
                    'last_updated': series_data.get('last_updated', ''),
                    'frequency': series_data.get('frequency', ''),
                    'units': series_data.get('units', ''),
                    'seasonal_adjustment': series_data.get('seasonal_adjustment', '')
                }),  # Store additional metadata as JSON
                'processed_timestamp': current_time,
                'source_file': str(metadata_file)
            }
            series_metadata.append(metadata)
        
        # Convert to DataFrame and ensure column order matches table schema
        df = pd.DataFrame(series_metadata)
        df = df[['series_id', 'series_name', 'series_type', 'is_valid', 'raw_data', 'raw_metadata', 'processed_timestamp', 'source_file']]
        
        # Insert data into ClickHouse
        client.insert_dataframe('bronze_series_metadata', df)
        
        logger.info(f"Inserted {len(df)} series metadata records into ClickHouse in {(datetime.now() - start_time).total_seconds():.2f} seconds")
        return len(df)
    
    except Exception as e:
        logger.error(f"Error loading series metadata to ClickHouse: {str(e)}")
        return 0

def load_series_values(bronze_dir: Path, client) -> int:
    """Load series values from bronze layer JSON files to ClickHouse."""
    data_file = bronze_dir / 'raw_series_data' / 'consolidated_data.json'
    
    if not data_file.exists():
        logger.warning(f"BLS data file not found: {data_file}")
        return 0
    
    try:
        start_time = datetime.now()
        logger.info(f"Loading series values from {data_file}")
        
        # Read the JSON file
        with open(data_file, 'r') as f:
            data = json.load(f)
        
        if not isinstance(data, dict):
            logger.error(f"Expected dictionary data, got {type(data)}")
            return 0
            
        # Extract series values
        series_values = []
        current_time = datetime.now()
        total_rows = 0
        
        for series_id, series_data in data.items():
            if not isinstance(series_data, dict):
                logger.warning(f"Invalid data format for series {series_id}: {type(series_data)}")
                continue
                
            # The data is nested under series_data['data']['data']
            series_observations = series_data.get('data', {})
            if not isinstance(series_observations, dict):
                logger.warning(f"Invalid observations format for series {series_id}: {type(series_observations)}")
                continue
                
            observations = series_observations.get('data', [])
            if not isinstance(observations, list):
                logger.warning(f"Invalid observations list for series {series_id}: {type(observations)}")
                continue
            
            for observation in observations:
                if not isinstance(observation, dict):
                    logger.warning(f"Invalid observation format for series {series_id}: {type(observation)}")
                    continue
                    
                try:
                    year = int(observation.get('year', 0))
                    if year == 0:
                        logger.warning(f"Missing year for series {series_id}")
                        continue
                        
                    # Convert footnotes to array of JSON objects
                    footnotes = observation.get('footnotes', [])
                    if not isinstance(footnotes, list):
                        footnotes = []
                    
                    # Convert each footnote to a JSON string
                    footnotes_json = [json.dumps(footnote) for footnote in footnotes]
                        
                    value = {
                        'series_id': series_id,
                        'year': year,
                        'period': str(observation.get('period', '')),
                        'period_name': str(observation.get('periodName', '')),
                        'value': str(observation.get('value', '')),
                        'footnotes': footnotes_json,  # Store as array of JSON strings
                        'raw_observation': json.dumps(observation),  # Store complete raw observation as JSON
                        'extraction_timestamp': current_time,
                        'source_file': str(data_file)
                    }
                    series_values.append(value)
                except Exception as e:
                    logger.warning(f"Error processing observation for series {series_id}: {str(e)}")
                    continue
            
            # Insert in batches of 100,000 records
            if len(series_values) >= 100000:
                try:
                    df = pd.DataFrame(series_values)
                    df = df[['series_id', 'year', 'period', 'period_name', 'value', 'footnotes', 'raw_observation', 'extraction_timestamp', 'source_file']]
                    client.insert_dataframe('bronze_series_values', df)
                    total_rows += len(df)
                    logger.info(f"Inserted batch of {len(df)} records (total: {total_rows})")
                except Exception as e:
                    logger.error(f"Error inserting DataFrame into bronze_series_values: {str(e)}")
                series_values = []
        
        # Insert remaining records
        if series_values:
            try:
                df = pd.DataFrame(series_values)
                df = df[['series_id', 'year', 'period', 'period_name', 'value', 'footnotes', 'raw_observation', 'extraction_timestamp', 'source_file']]
                client.insert_dataframe('bronze_series_values', df)
                total_rows += len(df)
                logger.info(f"Inserted final batch of {len(df)} records (total: {total_rows})")
            except Exception as e:
                logger.error(f"Error inserting DataFrame into bronze_series_values: {str(e)}")
        
        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Completed loading series values in {total_time:.2f} seconds")
        return total_rows
    
    except Exception as e:
        logger.error(f"Error loading series values to ClickHouse: {str(e)}")
        return 0

def main():
    """Run the bronze layer loading process."""
    logger.info("Starting bronze layer data loading")
    start_time = datetime.now()
    
    try:
        # Define data directory
        bronze_dir = project_root / "data" / "bronze"
        
        if not bronze_dir.exists():
            logger.error(f"Bronze data directory not found: {bronze_dir}")
            return
        
        # Get ClickHouse client
        with get_clickhouse_client() as client:
            # Load series metadata
            metadata_count = load_series_metadata(bronze_dir, client)
            
            # Load series values
            values_count = load_series_values(bronze_dir, client)
            
            # Log summary
            duration = (datetime.now() - start_time).total_seconds()
            logger.info("====== Loading Summary ======")
            logger.info(f"Total series metadata records loaded: {metadata_count}")
            logger.info(f"Total series values loaded: {values_count}")
            logger.info(f"Total execution time: {duration:.2f} seconds ({duration/60:.2f} minutes)")
            logger.info("============================")
        
    except Exception as e:
        logger.error(f"Error in bronze layer loading process: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("Bronze layer loading process completed")

if __name__ == "__main__":
    main() 