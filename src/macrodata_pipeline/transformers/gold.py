"""Gold layer transformer for the Macroeconomic Data Pipeline.

This module handles the transformation of silver layer data into the gold layer,
including aggregation, enrichment, and analytics-ready data preparation.
"""

import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import uuid

from macrodata_pipeline.utils import get_logger

logger = get_logger(__name__)

class GoldTransformer:
    """Transformer for converting silver layer data to gold layer format."""
    
    def __init__(
        self,
        silver_dir: str = "data/silver",
        gold_dir: str = "data/gold",
        log_dir: str = "logs/cleaning"
    ):
        """Initialize the gold transformer.
        
        Args:
            silver_dir: Directory containing silver layer data
            gold_dir: Directory for gold layer output
            log_dir: Directory for log files
        """
        self.silver_dir = Path(silver_dir)
        self.gold_dir = Path(gold_dir)
        self.log_dir = Path(log_dir)
        
        # Create output directories
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up logging
        self.logger = get_logger(
            __name__,
            log_file=self.log_dir / "gold_transformer.log"
        )
    
    def load_latest_silver_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Load the latest silver layer data files.
        
        Returns:
            Tuple of (metadata_df, series_df)
        """
        self.logger.info("Loading latest silver layer data")
        
        try:
            # Find metadata file
            metadata_file = self.silver_dir / "series_metadata.parquet"
            if not metadata_file.exists():
                raise FileNotFoundError("No silver metadata files found")
            
            # Find latest series file
            series_file = self.silver_dir / "bls_data" / "series_values.parquet"
            if not series_file.exists():
                # Try to find any parquet file in the bls_data directory
                series_files = list(self.silver_dir.glob("bls_data/*.parquet"))
                if not series_files:
                    raise FileNotFoundError("No silver series files found")
                series_file = series_files[0]
            
            # Load data
            metadata_df = pd.read_parquet(metadata_file)
            series_df = pd.read_parquet(series_file)
            
            self.logger.info(
                f"Loaded {len(metadata_df)} metadata records and "
                f"{len(series_df)} series records"
            )
            return metadata_df, series_df
            
        except Exception as e:
            self.logger.error(f"Error loading silver data: {str(e)}")
            raise
    
    def calculate_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate summary statistics for each series.
        
        Args:
            df: DataFrame containing series data
            
        Returns:
            DataFrame with calculated statistics
        """
        self.logger.info("Calculating series statistics")
        
        try:
            stats = df.groupby('series_id').agg({
                'value': [
                    'count',
                    'mean',
                    'std',
                    'min',
                    'max',
                    lambda x: x.iloc[-1],  # Latest value
                    lambda x: x.pct_change().mean() * 100,  # Average % change
                    lambda x: x.pct_change().std() * 100  # Volatility
                ]
            })
            
            # Flatten column names
            stats.columns = [
                'count',
                'mean',
                'std',
                'min',
                'max',
                'latest_value',
                'avg_pct_change',
                'volatility'
            ]
            
            # Reset index
            stats = stats.reset_index()
            
            # Add timestamp
            stats['calculated_at'] = pd.Timestamp.now(tz='UTC')
            
            self.logger.info(f"Calculated statistics for {len(stats)} series")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error calculating statistics: {str(e)}")
            raise
    
    def clean_series_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean series data by handling duplicates and ensuring proper types.
        
        Args:
            df: Raw series DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        self.logger.info("Cleaning series data")
        
        try:
            # Make sure we have a date column
            if 'date' not in df.columns:
                df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + 
                                         ((df['quarter'] * 3) - 2).astype(str).str.zfill(2) + '-01')
            
            # Convert value to float if needed
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            
            # Generate a unique identifier for each series and date combination
            df['unique_id'] = df['raw_series_id'] + '_' + df['date'].astype(str)
            
            # Keep only the most recent record for each unique_id
            df = df.sort_values('updated_at', ascending=False)
            df = df.drop_duplicates(subset=['unique_id'])
            
            # Drop the temporary column
            df = df.drop(columns=['unique_id'])
            
            self.logger.info(f"Cleaned series data: {len(df)} records remaining")
            return df
            
        except Exception as e:
            self.logger.error(f"Error cleaning series data: {str(e)}")
            raise

    def calculate_correlations(
        self,
        df: pd.DataFrame,
        min_periods: int = 12
    ) -> pd.DataFrame:
        """Calculate correlation matrix between series.
        
        Args:
            df: DataFrame containing series data
            min_periods: Minimum number of overlapping periods required
            
        Returns:
            DataFrame containing correlation matrix
        """
        self.logger.info("Calculating series correlations")
        
        try:
            # Clean the data first to remove duplicates
            clean_df = self.clean_series_data(df)
            
            # Pivot data for correlation calculation
            pivot_df = clean_df.pivot_table(
                index='date',
                columns='raw_series_id',  # Use raw_series_id instead of series_id
                values='value',
                aggfunc='mean'  # In case of any remaining duplicates
            )
            
            # Calculate correlations
            corr_matrix = pivot_df.corr(min_periods=min_periods)
            
            # Convert to long format
            corr_df = pd.DataFrame({
                'series_id_1': np.repeat(corr_matrix.index, len(corr_matrix.columns)),
                'series_id_2': np.tile(corr_matrix.columns, len(corr_matrix.index)),
                'correlation': corr_matrix.values.flatten()
            })
            
            # Remove self-correlations
            corr_df = corr_df[
                (corr_df['series_id_1'] != corr_df['series_id_2'])
            ]
            
            # Keep only pairs with significant correlation
            corr_df = corr_df[
                (corr_df['correlation'].abs() > 0.5)
            ]
            
            # Add timestamp
            corr_df['calculated_at'] = pd.Timestamp.now(tz='UTC')
            
            self.logger.info(f"Calculated {len(corr_df)} significant correlations")
            return corr_df
            
        except Exception as e:
            self.logger.error(f"Error calculating correlations: {str(e)}")
            raise
    
    def save_gold_data(
        self,
        metadata_df: pd.DataFrame,
        values_df: pd.DataFrame,
        corr_df: Optional[pd.DataFrame] = None,
        timestamp: str = None
    ) -> None:
        """Save transformed data to gold layer.
        
        Args:
            metadata_df: Enriched metadata DataFrame
            values_df: Values DataFrame
            corr_df: Optional correlations DataFrame
            timestamp: Optional timestamp for file names
        """
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Save metadata
            metadata_file = self.gold_dir / f"gold_metadata_{timestamp}.parquet"
            metadata_df.to_parquet(metadata_file)
            self.logger.info(f"Saved metadata to {metadata_file}")
            
            # Save values
            values_file = self.gold_dir / f"gold_values_{timestamp}.parquet"
            values_df.to_parquet(values_file)
            self.logger.info(f"Saved values to {values_file}")
            
            # Save correlations if provided
            if corr_df is not None:
                corr_file = self.gold_dir / f"gold_correlations_{timestamp}.parquet"
                corr_df.to_parquet(corr_file)
                self.logger.info(f"Saved correlations to {corr_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving gold data: {str(e)}")
            raise
    
    def transform(self) -> None:
        """Run the complete gold layer transformation process."""
        self.logger.info("Starting gold layer transformation")
        
        try:
            # Load silver data
            metadata_df, series_df = self.load_latest_silver_data()
            
            # Transform metadata to match gold schema
            gold_metadata = metadata_df.copy()
            gold_metadata['indicator_id'] = [str(uuid.uuid4()) for _ in range(len(gold_metadata))]
            gold_metadata = gold_metadata.rename(columns={
                'series_name': 'indicator_name',
                'series_type': 'indicator_type',
                'description': 'description',
                'frequency': 'frequency',
                'units': 'units',
                'seasonal_adjustment': 'seasonal_adjustment'
            })
            gold_metadata['category'] = gold_metadata['indicator_type']
            gold_metadata['subcategory'] = ''
            gold_metadata['created_at'] = pd.Timestamp.now(tz='UTC')
            gold_metadata['updated_at'] = pd.Timestamp.now(tz='UTC')
            
            # Keep only columns that match the schema
            gold_metadata = gold_metadata[[
                'indicator_id', 'series_id', 'raw_series_id', 'indicator_name',
                'indicator_type', 'description', 'frequency', 'units',
                'seasonal_adjustment', 'category', 'subcategory', 'created_at',
                'updated_at'
            ]]
            
            # Transform values to match gold schema
            gold_values = series_df.copy()
            gold_values['indicator_id'] = gold_metadata.set_index('series_id')['indicator_id'].reindex(gold_values['series_id']).values
            gold_values['extraction_timestamp'] = pd.Timestamp.now(tz='UTC')
            gold_values['created_at'] = pd.Timestamp.now(tz='UTC')
            gold_values['updated_at'] = pd.Timestamp.now(tz='UTC')
            
            # Keep only columns that match the schema
            gold_values = gold_values[[
                'indicator_id', 'series_id', 'raw_series_id', 'year', 'quarter',
                'value', 'footnotes', 'is_annual_avg', 'date', 'extraction_timestamp',
                'created_at', 'updated_at'
            ]]
            
            # Save gold data
            self.save_gold_data(gold_metadata, gold_values, None)
            
            self.logger.info("Completed gold layer transformation")
            
        except Exception as e:
            self.logger.error(f"Error in gold layer transformation: {str(e)}")
            raise 