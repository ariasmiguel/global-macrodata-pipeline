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
from datetime import datetime
from typing import Dict, List, Any
import glob

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
    2. Loads metadata into economic_indicators_metadata table
    3. Gets the generated UUIDs for mapping
    4. Reads and loads values from BLS data Parquet files
    5. Maps raw series IDs to UUIDs
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
        metadata_file = silver_dir / 'series_metadata.parquet'
        logger.info(f"Reading metadata from {metadata_file}")
        
        metadata_df = pd.read_parquet(metadata_file)
        logger.info(f"Loaded {len(metadata_df)} metadata records")
        
        # Get current timestamp for created_at and updated_at
        now = datetime.now()
        
        # Prepare metadata DataFrame for insertion
        metadata_df['source'] = 'BLS'
        metadata_df['industry_code'] = ''
        metadata_df['industry_name'] = ''
        metadata_df['seasonal_adjustment'] = ''
        metadata_df['base_period'] = ''
        metadata_df['created_at'] = now
        metadata_df['updated_at'] = now
        
        # Rename columns to match ClickHouse schema
        metadata_df = metadata_df.rename(columns={
            'series_id': 'raw_series_id',
            'series_name': 'indicator_name',
            'series_type': 'series_type'
        })
        
        # Select and order columns to match ClickHouse schema
        metadata_df = metadata_df[[
            'raw_series_id',
            'indicator_name',
            'source',
            'industry_code',
            'industry_name',
            'series_type',
            'seasonal_adjustment',
            'base_period',
            'created_at',
            'updated_at'
        ]]
        
        if not metadata_df.empty:
            logger.info(f"Preparing to insert {len(metadata_df)} metadata records")
            # Insert metadata using DataFrame
            client.insert_df('macro.economic_indicators_metadata', metadata_df)
            logger.info(f"Inserted {len(metadata_df)} metadata records")
            
            # Get the generated UUIDs for mapping
            logger.info("Fetching UUID mappings...")
            series_id_mapping: Dict[str, str] = {}
            result = client.query('''
                SELECT series_id, raw_series_id 
                FROM macro.economic_indicators_metadata
            ''')
            for row in result.result_rows:
                series_id_mapping[row[1]] = row[0]  # map raw_series_id to series_id
            logger.info(f"Found {len(series_id_mapping)} UUID mappings")
        
        # Load BLS data
        bls_dir = silver_dir / 'bls_data'
        # Get the most recent Parquet file
        parquet_files = list(bls_dir.glob('bls_data_*.parquet'))
        if not parquet_files:
            logger.error("No BLS data Parquet files found")
            return
            
        latest_file = max(parquet_files, key=lambda x: x.stat().st_mtime)
        logger.info(f"Processing {latest_file}")
        
        # Read Parquet file
        df = pd.read_parquet(latest_file)
        logger.info(f"Loaded {len(df)} records from BLS data")
        
        # Map series IDs to UUIDs
        df['series_id'] = df['series_id'].map(series_id_mapping)
        
        # Filter out records with no mapping
        unmapped = df['series_id'].isna()
        if unmapped.any():
            logger.warning(f"Found {unmapped.sum()} records with no mapping")
            df = df.dropna(subset=['series_id'])
        
        # Prepare DataFrame for insertion
        df['created_at'] = now
        df['updated_at'] = now
        
        # Select and order columns to match ClickHouse schema
        df = df[[
            'series_id',
            'date',
            'value',
            'is_annual_avg',
            'created_at',
            'updated_at'
        ]]
        
        if not df.empty:
            logger.info(f"Preparing to insert {len(df)} values")
            # Group data by year and insert each year separately
            df['year'] = pd.to_datetime(df['date']).dt.year
            for year in sorted(df['year'].unique()):
                year_data = df[df['year'] == year].copy()
                year_data = year_data.drop('year', axis=1)
                
                # Insert year data in smaller batches
                batch_size = 100000
                total_records = len(year_data)
                logger.info(f"Processing {total_records} records for year {year}")
                
                for i in range(0, total_records, batch_size):
                    batch = year_data.iloc[i:i + batch_size]
                    client.insert_df('macro.economic_indicators_values', batch)
                    logger.info(f"Inserted batch {i//batch_size + 1} ({len(batch)} records) for year {year}")
            
            logger.info(f"Inserted {len(df)} values from {latest_file}")
            
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise
    finally:
        client.close()

if __name__ == "__main__":
    # Create logs directory if it doesn't exist
    os.makedirs("logs/ingestion", exist_ok=True)
    load_silver_to_clickhouse() 