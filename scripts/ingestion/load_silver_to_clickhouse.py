"""
Script to load data from silver layer (Parquet files) into ClickHouse (gold layer).
This script handles the ingestion of processed economic indicators data into our
ClickHouse database using a star schema design.
"""

import os
import pandas as pd
from clickhouse_connect import get_client
from loguru import logger
from pathlib import Path
import uuid
from typing import Dict, List, Any

# Configure logger
logger.add(
    "logs/ingestion/load_silver_to_clickhouse.log",
    rotation="500 MB",
    retention="10 days",
    level="INFO"
)

def load_silver_to_clickhouse() -> None:
    """
    Load data from silver layer Parquet files into ClickHouse tables.
    
    This function:
    1. Reads metadata from series_metadata.parquet
    2. Generates UUIDs for each series
    3. Loads metadata into economic_indicators_metadata table
    4. Reads and loads values from BLS data Parquet files
    5. Maps raw series IDs to new UUIDs
    6. Loads values into economic_indicators_values table
    """
    # Initialize ClickHouse client
    client = get_client(
        host='localhost',
        port=8123,
        username='default',
        password='clickhouse'
    )
    
    try:
        # Load series metadata
        silver_dir = Path('data/silver')
        metadata_df = pd.read_parquet(silver_dir / 'series_metadata.parquet')
        
        # Prepare and insert metadata
        metadata_data: List[Dict[str, Any]] = []
        for _, row in metadata_df.iterrows():
            metadata_data.append({
                'series_id': str(uuid.uuid4()),  # Generate new UUID for each series
                'raw_series_id': row['series_id'],  # Store original BLS series ID
                'indicator_name': row['series_name'],
                'source': 'BLS',
                'industry_code': row.get('industry_code', ''),
                'industry_name': row.get('industry_name', ''),
                'series_type': row.get('series_type', ''),
                'seasonal_adjustment': row.get('seasonal_adjustment', ''),
                'base_period': row.get('base_period', '')
            })
        
        if metadata_data:
            client.insert('macro.economic_indicators_metadata', metadata_data)
            logger.info(f"Inserted {len(metadata_data)} metadata records")
            
            # Create a mapping of raw_series_id to our new series_id
            series_id_mapping: Dict[str, str] = {
                item['raw_series_id']: item['series_id'] 
                for item in metadata_data
            }
        
        # Load BLS data
        bls_dir = silver_dir / 'bls_data'
        for parquet_file in bls_dir.glob('*.parquet'):
            logger.info(f"Processing {parquet_file}")
            
            # Read Parquet file
            df = pd.read_parquet(parquet_file)
            
            # Prepare data for ClickHouse values table
            values_data: List[Dict[str, Any]] = []
            for _, row in df.iterrows():
                # Map the raw_series_id to our new series_id
                series_id = series_id_mapping.get(row['series_id'])
                if series_id:
                    values_data.append({
                        'series_id': series_id,
                        'date': row['date'],
                        'value': row['value']
                    })
                else:
                    logger.warning(f"No mapping found for raw_series_id: {row['series_id']}")
            
            # Insert data into ClickHouse
            if values_data:
                client.insert('macro.economic_indicators_values', values_data)
                logger.info(f"Inserted {len(values_data)} values from {parquet_file}")
            
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise
    finally:
        client.close()

if __name__ == "__main__":
    load_silver_to_clickhouse() 