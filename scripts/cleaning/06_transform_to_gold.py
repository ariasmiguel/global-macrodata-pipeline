#!/usr/bin/env python
"""Script to transform silver layer data into gold layer format.

This is step 6 in the Macroeconomic Data Pipeline - transforming silver data 
into aggregated and analytics-ready data in the gold layer.
"""

import os
import sys
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from macrodata_pipeline.transformers.gold import GoldTransformer
from macrodata_pipeline.utils import get_logger

# Set up logging
log_dir = project_root / "logs" / "cleaning"
log_dir.mkdir(parents=True, exist_ok=True)
logger = get_logger(
    __name__,
    log_file=log_dir / "06_transform_to_gold.log"
)

def main():
    """Run the gold layer transformation process."""
    logger.info("Starting gold layer transformation")
    
    try:
        # Add debugging transformer
        class DebugGoldTransformer(GoldTransformer):
            def calculate_correlations(self, df, min_periods=12):
                # Print dataframe info for debugging
                print("Series DataFrame columns:", df.columns.tolist())
                print("Series DataFrame head:")
                print(df.head())
                
                # Continue with parent implementation
                return super().calculate_correlations(df, min_periods)
        
        # Initialize transformer with debugging
        transformer = DebugGoldTransformer(
            silver_dir=project_root / "data" / "silver",
            gold_dir=project_root / "data" / "gold",
            log_dir=log_dir
        )
        
        # Run transformation
        transformer.transform()
        
        # Log completion
        logger.info("Completed gold layer transformation")
        
    except Exception as e:
        logger.error(f"Error in gold layer transformation: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 