import logging
from pathlib import Path
import pandas as pd
from typing import List, Dict
import requests
import time
import os
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# BLS API Configuration
BLS_API_KEY = os.getenv('BLS_API_KEY', '')  # Get API key from environment variable
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BATCH_SIZE = 50  # BLS allows up to 50 series per request
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
API_LIMIT = 500  # Daily API limit
SLEEP_BETWEEN_BATCHES = 0.75  # Increased to 750ms to be more conservative

class BLSAPITracker:
    def __init__(self, cache_file='.bls_api_count.json'):
        self.cache_file = cache_file
        self.today = datetime.now().date().isoformat()
        self._load_count()
    
    def _load_count(self):
        try:
            with open(self.cache_file, 'r') as f:
                data = json.load(f)
                if data.get('date') == self.today:
                    self.count = data.get('count', 0)
                else:
                    self.count = 0
        except (FileNotFoundError, json.JSONDecodeError):
            self.count = 0
    
    def _save_count(self):
        with open(self.cache_file, 'w') as f:
            json.dump({'date': self.today, 'count': self.count}, f)
    
    def increment(self):
        self.count += 1
        self._save_count()
        
    def can_make_request(self):
        return self.count < API_LIMIT
    
    def remaining(self):
        return API_LIMIT - self.count

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

def validate_series_batch(series_batch: List[str], retry_count: int = 0) -> Dict[str, bool]:
    """Validate a batch of series IDs against the BLS API with retries."""
    api_tracker = BLSAPITracker()
    
    if not api_tracker.can_make_request():
        logger.warning(f"Daily API limit reached. Remaining requests: 0")
        return {series_id: False for series_id in series_batch}
    
    headers = {'Content-type': 'application/json'}
    
    # Check current and previous year only
    current_year = datetime.now().year
    years = [str(current_year - i) for i in range(2)]  # Check last 2 years
    
    data = {
        "seriesid": series_batch,
        "startyear": years[-1],  # Use the older year as start
        "endyear": years[0],     # Use the newer year as end
        "registrationkey": BLS_API_KEY
    }
    
    try:
        response = requests.post(BLS_API_URL, json=data, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        validation_results = {}
        if result.get('status') == 'REQUEST_SUCCEEDED':
            series_data = result.get('Results', {}).get('series', [])
            for series in series_data:
                series_id = series.get('seriesID')
                # Consider a series valid if it exists in the response
                validation_results[series_id] = True
                logger.debug(f"Series {series_id} found in API response")
            
            # Mark missing series as invalid
            for series_id in series_batch:
                if series_id not in validation_results:
                    validation_results[series_id] = False
                    logger.debug(f"Series {series_id} not found in API response")
                    
            api_tracker.increment()
            logger.info(f"Remaining API requests today: {api_tracker.remaining()}")
        else:
            error_message = result.get('message', 'Unknown error')
            logger.warning(f"API request failed: {error_message}")
            logger.debug(f"Full API response: {result}")
            
            # Check for API limit message after logging the error
            if re.search(r'daily\s+threshold|limit|' + re.escape(BLS_API_KEY), error_message, re.IGNORECASE):
                logger.warning("Daily API limit reached. Stopping validation.")
                validation_results = {}
                
            if retry_count < MAX_RETRIES:
                logger.info(f"Retrying batch (attempt {retry_count + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
                return validate_series_batch(series_batch, retry_count + 1)
            else:
                logger.error(f"Max retries reached for batch. Marking as invalid.")
                validation_results = {}
        
        # Ensure all results are boolean values
        return {series_id: bool(is_valid) for series_id, is_valid in validation_results.items()}
    
    except Exception as e:
        logger.error(f"Error validating batch: {str(e)}")
        if retry_count < MAX_RETRIES:
            logger.info(f"Retrying batch (attempt {retry_count + 1}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY)
            return validate_series_batch(series_batch, retry_count + 1)
        else:
            logger.error(f"Max retries reached for batch. Marking as invalid.")
            return {series_id: False for series_id in series_batch}

def validate_all_series(series_ids: List[str]) -> Dict[str, bool]:
    """Validate all series IDs using batched requests."""
    all_results = {}
    start_time = time.time()
    api_tracker = BLSAPITracker()
    
    for i in range(0, len(series_ids), BATCH_SIZE):
        if not api_tracker.can_make_request():
            logger.warning("Daily API limit reached. Stopping validation.")
            break
            
        batch = series_ids[i:i + BATCH_SIZE]
        batch_start = time.time()
        logger.info(f"Validating batch {i//BATCH_SIZE + 1}/{(len(series_ids) + BATCH_SIZE - 1)//BATCH_SIZE}")
        
        batch_results = validate_series_batch(batch)
        all_results.update(batch_results)
        
        # If all results in batch are False, stop processing
        if all(value is False for value in batch_results.values()):
            logger.warning("API limit reached. Stopping all validation.")
            break
        
        batch_time = time.time() - batch_start
        logger.info(f"Batch {i//BATCH_SIZE + 1} took {batch_time:.2f} seconds")
        
        # Sleep between batches to respect rate limits
        time.sleep(SLEEP_BETWEEN_BATCHES)
    
    total_time = time.time() - start_time
    logger.info(f"Total validation time: {total_time:.2f} seconds")
    return all_results

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
        validation_results = validate_all_series(series_df['series_id'].tolist())
        
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