"""
Module for loading silver layer data into ClickHouse.
"""

import logging
from pathlib import Path
import pandas as pd
import uuid
from datetime import datetime
from typing import Optional, Tuple

from ..database.clickhouse import ClickHouseClient

logger = logging.getLogger(__name__)

class SilverLayerIngester:
    """Handles loading silver layer data into ClickHouse."""
    
    def __init__(self, silver_dir: Path, batch_size: int = 100000):
        """Initialize the ingester.
        
        Args:
            silver_dir: Directory containing silver layer data
            batch_size: Number of records to insert at once
        """
        self.silver_dir = silver_dir
        self.batch_size = batch_size
        self.client = ClickHouseClient()
    
    def _is_valid_uuid(self, val: str) -> bool:
        """Check if a string is a valid UUID."""
        try:
            uuid.UUID(str(val))
            return True
        except:
            return False
    
    def _convert_uuids(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert series_id column to UUID strings."""
        if df['series_id'].dtype == 'object':
            try:
                # If series_id is not a valid UUID, generate new UUIDs
                df['series_id'] = df['series_id'].apply(lambda x: 
                    str(uuid.UUID(x)) if self._is_valid_uuid(x) else str(uuid.uuid4()))
            except:
                # Generate new UUIDs if conversion fails
                df['series_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        return df
    
    def load_series_metadata(self) -> int:
        """Load series metadata from silver layer Parquet file to ClickHouse."""
        metadata_file = self.silver_dir / 'series_metadata.parquet'
        
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
            
            # Convert series_id to UUID
            df = self._convert_uuids(df)
            
            # Insert into ClickHouse
            self.client.execute_query("TRUNCATE TABLE silver_series_metadata")
            self.client.insert_dataframe('silver_series_metadata', df)
            
            logger.info(f"Inserted {len(df)} series metadata records into ClickHouse")
            return len(df)
        
        except Exception as e:
            logger.error(f"Error loading series metadata to ClickHouse: {str(e)}")
            return 0
    
    def load_series_values(self) -> int:
        """Load series values from silver layer Parquet files to ClickHouse."""
        values_dir = self.silver_dir / 'bls_data'
        
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
            self.client.execute_query("TRUNCATE TABLE silver_series_values")
            
            logger.info(f"Processing latest file: {latest_file}")
            
            # Read the Parquet file
            df = pd.read_parquet(latest_file)
            logger.info(f"Loaded {len(df)} records from {latest_file.name}")
            
            total_loaded = 0
            
            # Process in batches
            for i in range(0, len(df), self.batch_size):
                try:
                    batch = df.iloc[i:i+self.batch_size].copy()
                    
                    # Convert series_id to UUID
                    batch = self._convert_uuids(batch)
                    
                    # Convert boolean values to integers
                    batch['is_annual_avg'] = batch['is_annual_avg'].astype(int)
                    
                    # Convert timestamps to string format for ClickHouse
                    batch['created_at'] = pd.to_datetime(batch['created_at'])
                    batch['updated_at'] = pd.to_datetime(batch['updated_at'])
                    batch['extraction_timestamp'] = pd.to_datetime(batch['extraction_timestamp'])
                    
                    # Convert date to string format
                    batch['date'] = pd.to_datetime(batch['date'])
                    
                    # Insert into ClickHouse
                    self.client.insert_dataframe('silver_series_values', batch)
                    
                    total_loaded += len(batch)
                    logger.info(f"Inserted batch of {len(batch)} records (total: {total_loaded})")
                    
                except Exception as e:
                    logger.error(f"Error processing batch: {str(e)}")
            
            logger.info(f"Completed loading series values ({total_loaded} records)")
            return total_loaded
        
        except Exception as e:
            logger.error(f"Error loading series values to ClickHouse: {str(e)}")
            return 0
    
    def load(self) -> Tuple[int, int]:
        """Load all silver layer data into ClickHouse.
        
        Returns:
            Tuple of (metadata_count, values_count)
        """
        try:
            # Load series metadata
            metadata_count = self.load_series_metadata()
            
            # Load series values
            values_count = self.load_series_values()
            
            return metadata_count, values_count
            
        except Exception as e:
            logger.error(f"Error in loading process: {str(e)}")
            raise
        finally:
            self.client.close() 