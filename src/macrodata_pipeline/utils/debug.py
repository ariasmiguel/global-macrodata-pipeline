"""
Debugging utilities for the Macroeconomic Data Pipeline.
"""

import logging
from typing import Optional
import pandas as pd

from ..transformers.gold import GoldTransformer

logger = logging.getLogger(__name__)

class DebugGoldTransformer(GoldTransformer):
    """Debug version of GoldTransformer that adds detailed logging."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logger.info("Initialized DebugGoldTransformer")
    
    def calculate_correlations(self, df: pd.DataFrame, min_periods: int = 12) -> pd.DataFrame:
        """Calculate correlations with detailed logging.
        
        Args:
            df: DataFrame containing series data
            min_periods: Minimum number of periods required for correlation
            
        Returns:
            DataFrame with correlation results
        """
        logger.info("Calculating correlations...")
        logger.info(f"Input DataFrame shape: {df.shape}")
        logger.info(f"Input DataFrame columns: {df.columns.tolist()}")
        logger.info("Input DataFrame head:")
        logger.info(f"\n{df.head()}")
        
        try:
            result = super().calculate_correlations(df, min_periods)
            logger.info("Correlation calculation successful")
            logger.info(f"Output DataFrame shape: {result.shape}")
            logger.info(f"Output DataFrame columns: {result.columns.tolist()}")
            return result
        except Exception as e:
            logger.error(f"Error calculating correlations: {str(e)}")
            logger.error(f"DataFrame info:\n{df.info()}")
            raise
    
    def transform(self) -> None:
        """Run transformation with detailed logging."""
        logger.info("Starting gold layer transformation")
        try:
            super().transform()
            logger.info("Gold layer transformation completed successfully")
        except Exception as e:
            logger.error(f"Error in gold layer transformation: {str(e)}")
            raise 