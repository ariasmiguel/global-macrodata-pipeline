"""
Script to generate and validate PPI series IDs for further processing.
This is step 1 in the Macroeconomic Data Pipeline - generating and validating series metadata.
"""

import logging
from pathlib import Path
import pandas as pd
import time
from datetime import datetime
import os

from macrodata_pipeline.extractors.bls import BLSExtractor

# Configure logging
os.makedirs("logs/ingestion", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/ingestion/01_generate_ppi_series.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_ppi_series() -> pd.DataFrame:
    """Load PPI series from the combined dataset."""
    bronze_dir = Path("data/bronze")
    series_df = pd.read_csv(bronze_dir / "final_ppi_series_ids.csv")
    return series_df

def validate_series_by_type(series_df: pd.DataFrame, extractor: BLSExtractor) -> pd.DataFrame:
    """Validate series IDs by type and update dataframe."""
    # Initialize validation column
    series_df['is_valid'] = False
    
    # Process each type separately for better logging
    for series_type in series_df['series_type'].unique():
        type_series = series_df[series_df['series_type'] == series_type]
        logger.info(f"\nValidating {len(type_series)} {series_type} series...")
        
        # Validate in batches to respect API rate limits
        series_ids = type_series['series_id'].tolist()
        validation_results = extractor.validate_all_series(series_ids)
        
        # Update results in the dataframe
        for series_id, result in validation_results.items():
            mask = series_df['series_id'] == series_id
            if isinstance(result, dict):
                series_df.loc[mask, 'is_valid'] = True
            else:
                series_df.loc[mask, 'is_valid'] = bool(result)
        
        # Log validation results for this type
        valid_count = type_series['is_valid'].sum()
        logger.info(f"Found {valid_count} valid {series_type} series out of {len(type_series)}")
    
    return series_df

def main():
    """Main function to validate PPI series IDs."""
    try:
        start_time = time.time()
        
        logger.info("Loading PPI series data...")
        series_df = load_ppi_series()
        
        logger.info("Validating series IDs against BLS API...")
        extractor = BLSExtractor()
        series_df = validate_series_by_type(series_df, extractor)
        
        # Save results
        output_path = Path("data/bronze/validated_ppi_series.csv")
        series_df.to_csv(output_path, index=False)
        
        # Log summary statistics
        total_time = time.time() - start_time
        valid_count = series_df['is_valid'].sum()
        total_count = len(series_df)
        
        logger.info(f"\nValidation Summary:")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        logger.info(f"Total series: {total_count}")
        logger.info(f"Valid series: {valid_count}")
        logger.info(f"Invalid series: {total_count - valid_count}")
        
        # Log validation results by type
        logger.info("\nResults by series type:")
        for series_type in series_df['series_type'].unique():
            type_df = series_df[series_df['series_type'] == series_type]
            valid = type_df['is_valid'].sum()
            total = len(type_df)
            logger.info(f"{series_type}: {valid}/{total} valid ({(valid/total)*100:.1f}%)")
        
        logger.info(f"\nResults saved to {output_path}")
        
    except Exception as e:
        logger.error(f"Error validating series IDs: {str(e)}")
        raise

if __name__ == "__main__":
    main() 