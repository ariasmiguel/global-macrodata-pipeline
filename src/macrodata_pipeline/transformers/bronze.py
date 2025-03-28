"""Bronze layer data transformation utilities.

This module provides utilities for loading and transforming data from the bronze layer
into a format suitable for loading into ClickHouse.
"""

import pandas as pd
import logging
import json
from pathlib import Path
from typing import Dict, Any, Tuple, List
from datetime import datetime

from macrodata_pipeline.database.clickhouse import ClickHouseClient
from macrodata_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

class BronzeTransformer:
    """Handles transformation of bronze layer data."""
    
    def __init__(self, bronze_dir: Path):
        """Initialize the bronze transformer.
        
        Args:
            bronze_dir: Path to the bronze layer directory
        """
        self.bronze_dir = Path(bronze_dir)
        self.metadata_path = self.bronze_dir / "validated_ppi_series.csv"
        self.values_path = self.bronze_dir / "raw_series_data" / "consolidated_data.json"
        
    def load_metadata(self) -> pd.DataFrame:
        """Load series metadata from the bronze layer.
        
        Returns:
            DataFrame containing metadata
            
        Raises:
            FileNotFoundError: If metadata file doesn't exist
        """
        logger.info(f"Reading metadata from {self.metadata_path}")
        
        if not self.metadata_path.exists():
            logger.error(f"Metadata file not found: {self.metadata_path}")
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_path}")
        
        df = pd.read_csv(self.metadata_path)
        logger.info(f"Loaded {len(df)} metadata records")
        return df
    
    def load_series_values(self) -> Dict[str, Any]:
        """Load series values from the bronze layer.
        
        Returns:
            Dictionary containing series values
            
        Raises:
            FileNotFoundError: If values file doesn't exist
        """
        logger.info(f"Reading values from {self.values_path}")
        
        if not self.values_path.exists():
            logger.error(f"Values file not found: {self.values_path}")
            raise FileNotFoundError(f"Values file not found: {self.values_path}")
        
        with open(self.values_path, 'r') as f:
            data = json.load(f)
        
        logger.info(f"Loaded values for {len(data)} series")
        return data
    
    def prepare_metadata_for_insertion(self, metadata_df: pd.DataFrame) -> pd.DataFrame:
        """Prepare metadata DataFrame for insertion into ClickHouse.
        
        Args:
            metadata_df: Raw metadata DataFrame
            
        Returns:
            Processed DataFrame ready for insertion
        """
        # Add timestamps
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        metadata_df['created_at'] = now
        metadata_df['updated_at'] = now
        
        # Ensure all required columns exist
        required_columns = [
            'series_id', 'series_name', 'series_type', 'is_valid',
            'processed_timestamp', 'created_at', 'updated_at'
        ]
        
        for col in required_columns:
            if col not in metadata_df.columns:
                logger.warning(f"Missing required column: {col}")
                metadata_df[col] = None
        
        return metadata_df[required_columns]
    
    def prepare_values_for_insertion(self, values_data: Dict[str, Any]) -> pd.DataFrame:
        """Prepare series values for insertion into ClickHouse.
        
        Args:
            values_data: Dictionary containing series values
            
        Returns:
            DataFrame ready for insertion
        """
        # Convert dictionary to DataFrame
        rows = []
        for series_id, series_data in values_data.items():
            for date, value in series_data.items():
                rows.append({
                    'series_id': series_id,
                    'date': date,
                    'value': value,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
        
        df = pd.DataFrame(rows)
        logger.info(f"Prepared {len(df)} value records for insertion")
        return df
    
    def load_to_clickhouse(self, client: ClickHouseClient) -> Tuple[int, int]:
        """Load bronze layer data into ClickHouse.
        
        Args:
            client: ClickHouse client instance
            
        Returns:
            Tuple of (metadata_count, values_count)
        """
        try:
            # Load and insert metadata
            metadata_df = self.load_metadata()
            metadata_df = self.prepare_metadata_for_insertion(metadata_df)
            metadata_count = client.insert_dataframe(
                'macro.bronze_series_metadata',
                metadata_df
            )
            logger.info(f"Inserted {metadata_count} metadata records")
            
            # Load and insert values
            values_data = self.load_series_values()
            values_df = self.prepare_values_for_insertion(values_data)
            values_count = client.insert_dataframe(
                'macro.bronze_series_values',
                values_df
            )
            logger.info(f"Inserted {values_count} value records")
            
            return metadata_count, values_count
            
        except Exception as e:
            logger.error(f"Error loading bronze data to ClickHouse: {str(e)}")
            raise 