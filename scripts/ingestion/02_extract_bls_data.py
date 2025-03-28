"""
Script to extract data from the Bureau of Labor Statistics (BLS) API.
This is step 2 in the Macroeconomic Data Pipeline - extracting raw data from BLS.
"""

import logging
from pathlib import Path
import pandas as pd
from datetime import datetime
import os

from macrodata_pipeline.extractors.bls import BLSExtractor
from macrodata_pipeline.extractors.batch_processor import BLSBatchProcessor

# Configure logging
os.makedirs("logs/ingestion", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/ingestion/02_extract_bls_data.log')
    ]
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set logger to DEBUG level to see all messages

START_YEAR = 2010
END_YEAR = datetime.now().year

def load_validated_series(series_type: str = None) -> pd.DataFrame:
    """Load validated PPI series, optionally filtered by type.
    
    Args:
        series_type: Optional filter for 'aggregated', 'industry', or 'commodity'
    """
    bronze_dir = Path("data/bronze")
    series_df = pd.read_csv(bronze_dir / "validated_ppi_series.csv")
    if series_type:
        return series_df[series_df['series_type'] == series_type]
    return series_df

def main():
    """Main function to extract BLS data."""
    try:
        logger.info("Starting BLS data extraction")
        start_time = datetime.now()
        
        # Initialize extractor and batch processor
        extractor = BLSExtractor()
        processor = BLSBatchProcessor(extractor)
        
        # Load series to process
        series_df = load_validated_series()
        logger.info(f"Loaded {len(series_df)} series to process")
        
        # Extract data
        results = processor.extract_series_data(series_df, START_YEAR, END_YEAR)
        
        # Log summary
        success_count = sum(1 for r in results.values() if r['status'] == 'success')
        error_count = sum(1 for r in results.values() if r['status'] == 'error')
        
        logger.info(f"Extraction completed in {datetime.now() - start_time}")
        logger.info(f"Summary: {success_count} successful, {error_count} failed")
        
        if error_count > 0:
            logger.warning("Some series failed to extract. Check logs for details.")
        
    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}")
        raise

if __name__ == "__main__":
    main() 