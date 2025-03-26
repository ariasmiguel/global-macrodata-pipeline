from pathlib import Path
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def download_ppi_codes():
    """Download and process PPI industry and product codes from BLS."""
    # Initialize paths
    data_dir = Path("data")
    bronze_dir = data_dir / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    # URLs for PPI data
    ppi_url = "https://download.bls.gov/pub/time.series/pc/pc.product"
    industry_url = "https://download.bls.gov/pub/time.series/pc/pc.industry"
    
    try:
        # Download product codes
        logger.info("Downloading product codes...")
        response = requests.get(ppi_url)
        response.raise_for_status()
        
        # Process product codes
        product_df = pd.read_csv(ppi_url, sep='\t')
        product_df = product_df[['product_code', 'product_name']]
        product_df.to_csv(bronze_dir / "ppi_product_codes.csv", index=False)
        logger.info(f"Saved {len(product_df)} product codes")
        
        # Download industry codes
        logger.info("Downloading industry codes...")
        response = requests.get(industry_url)
        response.raise_for_status()
        
        # Process industry codes
        industry_df = pd.read_csv(industry_url, sep='\t')
        industry_df = industry_df[['industry_code', 'industry_name']]
        industry_df.to_csv(bronze_dir / "ppi_industry_codes.csv", index=False)
        logger.info(f"Saved {len(industry_df)} industry codes")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading data: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

if __name__ == "__main__":
    download_ppi_codes() 