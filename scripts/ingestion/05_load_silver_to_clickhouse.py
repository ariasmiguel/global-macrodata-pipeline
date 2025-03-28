"""
Script to load data from silver layer files into ClickHouse silver tables.
This is step 5 in the Macroeconomic Data Pipeline - loading cleaned data into the ClickHouse silver layer.
"""

import os
import sys
import logging
from pathlib import Path
import time
from datetime import datetime
import json

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from macrodata_pipeline.ingesters.silver import SilverLayerIngester
from macrodata_pipeline.utils import get_logger

# Set up logging
log_dir = project_root / "logs" / "ingestion"
log_dir.mkdir(parents=True, exist_ok=True)
logger = get_logger(
    __name__,
    log_file=log_dir / "05_load_silver_to_clickhouse.log"
)

def main():
    """Main execution function."""
    start_time = time.time()
    
    # Define data directory
    silver_dir = project_root / "data" / "silver"
    
    if not silver_dir.exists():
        logger.error(f"Silver data directory not found: {silver_dir}")
        return
    
    logger.info(f"Starting silver layer data loading to ClickHouse")
    
    try:
        # Initialize ingester
        ingester = SilverLayerIngester(silver_dir)
        
        # Load data
        metadata_count, values_count = ingester.load()
        
        # Log summary
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info("====== Loading Summary ======")
        logger.info(f"Total series metadata records loaded: {metadata_count}")
        logger.info(f"Total series values loaded: {values_count}")
        logger.info(f"Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        logger.info("============================")
        
        # Save results to a summary file
        summary = {
            "timestamp": datetime.now().isoformat(),
            "metadata_records_loaded": metadata_count,
            "values_loaded": values_count,
            "execution_time_seconds": round(total_time, 2)
        }
        
        summary_dir = project_root / "data" / "logs"
        summary_dir.mkdir(parents=True, exist_ok=True)
        
        with open(summary_dir / "silver_loading_summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        
    except Exception as e:
        logger.error(f"Error in loading process: {str(e)}")
        logger.error("Full error details:", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Silver layer loading process completed")

if __name__ == "__main__":
    main() 