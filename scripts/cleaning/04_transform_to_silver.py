#!/usr/bin/env python
"""Script to transform bronze layer data into silver layer format.

This is step 4 in the Macroeconomic Data Pipeline - transforming raw data
into cleaned and standardized datasets in the silver layer.
"""

import os
import sys
from pathlib import Path
import logging
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from macrodata_pipeline.transformers.silver import SilverTransformer
from macrodata_pipeline.utils import get_logger

# Set up logging
log_dir = project_root / "logs" / "cleaning"
log_dir.mkdir(parents=True, exist_ok=True)
logger = get_logger(
    __name__,
    log_file=log_dir / "04_transform_to_silver.log"
)

def main():
    """Run the silver layer transformation process."""
    logger.info("Starting silver layer transformation")
    start_time = datetime.now()
    
    try:
        # Initialize transformer
        transformer = SilverTransformer(
            bronze_dir=project_root / "data" / "bronze",
            silver_dir=project_root / "data" / "silver",
            log_dir=log_dir
        )
        
        # Run transformation
        metadata_count, values_count = transformer.transform()
        
        # Log completion
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Completed silver layer transformation in {duration:.2f} seconds")
        logger.info(f"Processed {metadata_count} metadata records and {values_count} value records")
        
        # Log file locations
        silver_dir = project_root / "data" / "silver"
        logger.info(f"Silver layer files:")
        logger.info(f"- Metadata: {silver_dir / 'series_metadata.parquet'}")
        logger.info(f"- Values: {silver_dir / 'bls_data'}")
        
    except Exception as e:
        logger.error(f"Error in silver layer transformation: {str(e)}")
        logger.error("Full error details:", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 