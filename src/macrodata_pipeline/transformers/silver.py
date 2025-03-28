"""Silver layer transformer for cleaning and standardizing data."""

import os
import sys
from pathlib import Path
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Tuple
import json
import uuid
import shutil

# Add project root to Python path
project_root = Path(__file__).resolve().parents[3]
sys.path.append(str(project_root))

from macrodata_pipeline.utils import get_logger
from macrodata_pipeline.database.clickhouse import get_clickhouse_client

class SilverTransformer:
    """Transformer for cleaning and standardizing data in the silver layer."""
    
    def __init__(
        self,
        bronze_dir: Path,
        silver_dir: Path,
        log_dir: Path
    ):
        """Initialize the silver layer transformer.
        
        Args:
            bronze_dir: Path to bronze layer data directory
            silver_dir: Path to silver layer data directory
            log_dir: Path to log directory
        """
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        self.log_dir = log_dir
        
        # Set up logging
        self.logger = get_logger(
            __name__,
            log_file=log_dir / "silver_transformer.log"
        )
        
        # Create output directories
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        (self.silver_dir / 'bls_data').mkdir(exist_ok=True)
    
    def process_metadata(self, client) -> int:
        """Process and clean series metadata.
        
        Args:
            client: ClickHouse client
            
        Returns:
            Number of processed metadata records
        """
        try:
            start_time = datetime.now()
            self.logger.info("Processing series metadata")
            
            # Query bronze metadata with DISTINCT to remove duplicates
            query = """
                SELECT DISTINCT
                    series_id as raw_series_id,
                    series_name,
                    series_type,
                    is_valid,
                    raw_data,
                    processed_timestamp,
                    source_file
                FROM macro.bronze_series_metadata
            """
            
            result = client.execute_query(query)
            if not result:
                self.logger.warning("No metadata records found in bronze layer")
                return 0
                
            # Convert to DataFrame
            df = pd.DataFrame(result, columns=[
                'raw_series_id', 'series_name', 'series_type', 'is_valid',
                'raw_data', 'processed_timestamp', 'source_file'
            ])
            
            # Extract fields from raw_data
            df['source_id'] = 'BLS_API'  # Default source
            df['description'] = df['series_type']  # Use series_type as description
            df['frequency'] = 'monthly'  # All data is monthly
            df['units'] = 'index'  # All data is index values
            df['seasonal_adjustment'] = 'unadjusted'  # Default
            
            # Generate new UUID for series_id
            df['series_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
            
            # Convert processed_timestamp to datetime
            df['processed_timestamp'] = pd.to_datetime(df['processed_timestamp'])
            
            # Update timestamps
            now = datetime.now()
            df['created_at'] = now
            df['updated_at'] = now
            
            # Select and order columns for output
            output_columns = [
                'series_id', 'raw_series_id', 'source_id', 'series_name', 'series_type',
                'description', 'frequency', 'units', 'seasonal_adjustment', 'is_valid',
                'processed_timestamp', 'created_at', 'updated_at'
            ]
            df = df[output_columns]
            
            # Save to Parquet
            output_file = self.silver_dir / 'series_metadata.parquet'
            df.to_parquet(output_file, index=False)
            
            # Insert into ClickHouse with FINAL to ensure deduplication
            client.execute_query("TRUNCATE TABLE macro.silver_series_metadata")
            client.insert_dataframe('macro.silver_series_metadata', df)
            
            self.logger.info(f"Processed {len(df)} metadata records in {(datetime.now() - start_time).total_seconds():.2f} seconds")
            return len(df)
        
        except Exception as e:
            self.logger.error(f"Error processing metadata: {str(e)}")
            raise
    
    def process_values(self, client) -> int:
        """Process and clean series values.
        
        Args:
            client: ClickHouse client
            
        Returns:
            Number of processed value records
        """
        try:
            start_time = datetime.now()
            self.logger.info("Processing series values")
            
            # Query bronze values with DISTINCT to remove duplicates
            query = """
                SELECT DISTINCT
                    series_id as raw_series_id,
                    year,
                    period,
                    period_name,
                    value,
                    footnotes,
                    raw_observation,
                    extraction_timestamp,
                    source_file
                FROM macro.bronze_series_values
            """
            
            result = client.execute_query(query)
            if not result:
                self.logger.warning("No value records found in bronze layer")
                return 0
                
            # Convert to DataFrame
            df = pd.DataFrame(result, columns=[
                'raw_series_id', 'year', 'period', 'period_name', 'value',
                'footnotes', 'raw_observation', 'extraction_timestamp', 'source_file'
            ])
            
            # Get clean series IDs from metadata with DISTINCT and FINAL
            metadata_query = """
                SELECT DISTINCT raw_series_id, series_id
                FROM macro.silver_series_metadata FINAL
            """
            metadata_result = client.execute_query(metadata_query)
            metadata_df = pd.DataFrame(metadata_result, columns=['raw_series_id', 'series_id'])
            
            # Merge with metadata to get clean series IDs
            df = df.merge(metadata_df, on='raw_series_id', how='left')
            
            # Convert period to quarter
            df['quarter'] = df['period'].apply(lambda x: (int(x[1:]) + 2) // 3 if x.startswith('M') else None)
            
            # Convert value to float
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            
            # Convert footnotes to string
            df['footnotes'] = df['footnotes'].apply(lambda x: x if isinstance(x, str) else json.dumps(x))
            
            # Calculate is_annual_avg flag
            df['is_annual_avg'] = df['period'].apply(lambda x: 1 if x == 'M13' else 0)
            
            # Create date column
            df['date'] = pd.to_datetime(
                df['year'].astype(str) + '-' + df['period'].str[1:].str.zfill(2) + '-01',
                format='%Y-%m-%d',
                errors='coerce'
            )
            
            # Convert extraction_timestamp to datetime
            df['extraction_timestamp'] = pd.to_datetime(df['extraction_timestamp'])
            
            # Update timestamps
            now = datetime.now()
            df['created_at'] = now
            df['updated_at'] = now
            
            # Select and order columns for output
            output_columns = [
                'series_id', 'raw_series_id', 'year', 'quarter', 'value',
                'footnotes', 'is_annual_avg', 'date', 'extraction_timestamp',
                'created_at', 'updated_at'
            ]
            df = df[output_columns]
            
            # Convert UUIDs to strings for Parquet compatibility
            df_parquet = df.copy()
            df_parquet['series_id'] = df_parquet['series_id'].astype(str)
            df_parquet['raw_series_id'] = df_parquet['raw_series_id'].astype(str)
            
            # Generate timestamp for file name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save to Parquet with timestamp in filename
            output_file = self.silver_dir / 'bls_data' / f'bls_data_{timestamp}.parquet'
            df_parquet.to_parquet(output_file, index=False)
            
            # Create a symlink to the latest file
            latest_file = self.silver_dir / 'bls_data' / 'series_values.parquet'
            if latest_file.exists():
                latest_file.unlink()  # Remove existing symlink if it exists
            latest_file.symlink_to(output_file.relative_to(latest_file.parent))
            
            # Clean up old files (keep only the latest 5)
            bls_data_dir = self.silver_dir / 'bls_data'
            old_files = sorted(
                [f for f in bls_data_dir.glob('bls_data_*.parquet')],
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )[5:]  # Skip the 5 most recent files
            
            for old_file in old_files:
                try:
                    old_file.unlink()
                    self.logger.info(f"Removed old file: {old_file}")
                except Exception as e:
                    self.logger.warning(f"Failed to remove old file {old_file}: {str(e)}")
            
            # Insert into ClickHouse with FINAL to ensure deduplication
            client.execute_query("TRUNCATE TABLE macro.silver_series_values")
            client.insert_dataframe('macro.silver_series_values', df)
            
            self.logger.info(f"Processed {len(df)} value records in {(datetime.now() - start_time).total_seconds():.2f} seconds")
            return len(df)
        
        except Exception as e:
            self.logger.error(f"Error processing values: {str(e)}")
            raise
    
    def transform(self) -> Tuple[int, int]:
        """Transform bronze layer data to silver layer.
        
        Returns:
            Tuple of (number of metadata records processed, number of value records processed)
        """
        try:
            self.logger.info("Starting silver layer transformation")
            start_time = datetime.now()
            
            # Get ClickHouse client
            with get_clickhouse_client() as client:
                # Process metadata
                metadata_count = self.process_metadata(client)
                
                # Process values
                values_count = self.process_values(client)
                
                # Log summary
                duration = (datetime.now() - start_time).total_seconds()
                self.logger.info("====== Transformation Summary ======")
                self.logger.info(f"Total metadata records processed: {metadata_count}")
                self.logger.info(f"Total value records processed: {values_count}")
                self.logger.info(f"Total execution time: {duration:.2f} seconds ({duration/60:.2f} minutes)")
                self.logger.info("==================================")
                
                return metadata_count, values_count
        
        except Exception as e:
            self.logger.error(f"Error in silver transformation: {str(e)}")
            raise
        finally:
            self.logger.info("Silver layer transformation completed") 