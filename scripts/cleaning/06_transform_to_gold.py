#!/usr/bin/env python
"""Script to transform silver layer data into gold layer format.

This is step 6 in the Macroeconomic Data Pipeline - transforming silver data 
into aggregated and analytics-ready data in the gold layer.
"""

import os
import sys
from pathlib import Path
import logging
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from macrodata_pipeline.utils.debug import DebugGoldTransformer
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
    start_time = datetime.now()
    
    try:
        # Initialize transformer with debugging
        transformer = DebugGoldTransformer(
            input_dir=project_root / "data" / "silver",
            output_dir=project_root / "data" / "gold"
        )
        
        # Run transformation
        transformer.transform()
        
        # Log completion
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Completed gold layer transformation in {duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error in gold layer transformation: {str(e)}")
        logger.error("Full error details:", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 