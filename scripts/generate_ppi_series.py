from pathlib import Path
import logging
from macrodata_pipeline.extractors.bls import BLSExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    try:
        # Initialize paths
        data_dir = Path("data")
        bronze_dir = data_dir / "bronze"
        
        industry_codes_path = bronze_dir / "ppi_industry_codes.csv"
        product_codes_path = bronze_dir / "ppi_product_codes.csv"
        output_path = bronze_dir / "ppi_series_ids.txt"
        
        # Check if input files exist
        if not industry_codes_path.exists():
            raise FileNotFoundError(f"Industry codes file not found: {industry_codes_path}")
        if not product_codes_path.exists():
            raise FileNotFoundError(f"Product codes file not found: {product_codes_path}")
        
        # Initialize extractor
        extractor = BLSExtractor()
        
        # Generate series IDs
        logger.info("Generating PPI series IDs...")
        series_ids = extractor.generate_ppi_series_ids(
            industry_codes_path=industry_codes_path,
            product_codes_path=product_codes_path
        )
        logger.info(f"Generated {len(series_ids)} potential series IDs")
        
        # Validate series IDs
        logger.info("Validating series IDs (this may take a while)...")
        valid_series = extractor.validate_ppi_series(series_ids)
        logger.info(f"Found {len(valid_series)} valid series IDs")
        
        # Save valid series IDs
        extractor.save_ppi_series_ids(valid_series, output_path)
        logger.info(f"Saved valid series IDs to {output_path}")
        
        # Log sample of valid series
        if valid_series:
            logger.info("Sample of valid series IDs:")
            for series_id in valid_series[:5]:
                logger.info(f"  {series_id}")
                
    except Exception as e:
        logger.error(f"Error processing PPI series: {str(e)}")
        raise

if __name__ == "__main__":
    main() 