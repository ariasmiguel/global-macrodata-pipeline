import logging
from pathlib import Path
import pandas as pd
from typing import List, Dict
import time
from datetime import datetime

from macrodata_pipeline.extractors.bls import BLSExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_ppi_codes() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load industry and product codes from CSV files."""
    bronze_dir = Path("data/bronze")
    
    industry_codes = pd.read_csv(bronze_dir / "ppi_industry_codes.csv")
    product_codes = pd.read_csv(bronze_dir / "ppi_product_codes.csv")
    
    return industry_codes, product_codes

def generate_series_ids(industry_codes: pd.DataFrame, product_codes: pd.DataFrame) -> List[Dict]:
    """Generate all possible PPI series IDs."""
    series_list = []
    
    for _, industry in industry_codes.iterrows():
        industry_code = industry['industry_code'].strip()
        
        matching_products = product_codes[product_codes['industry_code'].str.strip() == industry_code]
        
        for _, product in matching_products.iterrows():
            product_code = product['product_code'].strip()
            
            series_id = f"PCU{industry_code}{product_code}"
            
            series_list.append({
                'series_id': series_id,
                'industry_code': industry_code,
                'industry_name': industry['industry_name'],
                'product_code': product_code,
                'product_name': product['product_name']
            })
    
    return series_list

def main():
    """Main function to generate and save PPI series IDs."""
    try:
        start_time = time.time()
        
        logger.info("Loading PPI codes...")
        industry_codes, product_codes = load_ppi_codes()
        
        logger.info("Generating series IDs...")
        series_list = generate_series_ids(industry_codes, product_codes)
        
        series_df = pd.DataFrame(series_list)
        
        logger.info("Validating series IDs against BLS API...")
        extractor = BLSExtractor()
        validation_results = extractor.validate_all_series(series_df['series_id'].tolist())
        
        # Ensure all validation results are boolean values
        series_df['is_valid'] = series_df['series_id'].map(lambda x: bool(validation_results.get(x, False)))
        
        output_path = Path("data/bronze/ppi_series_ids.csv")
        series_df.to_csv(output_path, index=False)
        
        valid_count = series_df['is_valid'].sum()
        total_count = len(series_df)
        total_time = time.time() - start_time
        
        logger.info(f"\nSummary:")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        logger.info(f"Generated {total_count} series IDs ({valid_count} valid)")
        logger.info(f"Saved to {output_path}")
        
        # Log valid series IDs for verification
        valid_series = series_df[series_df['is_valid']]
        logger.info("\nValid series IDs:")
        logger.info(valid_series[['series_id', 'industry_name', 'product_name']].to_string())
        
    except Exception as e:
        logger.error(f"Error generating series IDs: {str(e)}")
        raise

if __name__ == "__main__":
    main() 