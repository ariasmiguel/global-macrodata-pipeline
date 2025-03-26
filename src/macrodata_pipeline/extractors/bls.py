from typing import Any, Dict, List, Optional, Tuple
import time
import itertools
import logging
from pathlib import Path
import json
import re
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
import os

from .base import BaseExtractor
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
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

class BLSExtractor(BaseExtractor):
    """Extractor for BLS API data."""
    
    BASE_URL = "https://api.bls.gov/publicAPI/v2"
    
    def __init__(self, api_key: Optional[str] = None):
        load_dotenv()
        self.api_key = api_key or os.getenv("BLS_API_KEY")
        if not self.api_key:
            raise ValueError("BLS API key is required")
        super().__init__()
        self.api_tracker = BLSAPITracker()
        logger.debug("Initialized BLSExtractor")
    
    def generate_ppi_series_ids(self, industry_codes_path: Path, product_codes_path: Path) -> List[str]:
        """Generate PPI series IDs from industry and product codes."""
        logger.debug(f"Loading industry codes from {industry_codes_path}")
        industry_codes = pd.read_csv(industry_codes_path)['industry_code'].astype(str).str.zfill(6).tolist()
        logger.debug(f"Found {len(industry_codes)} industry codes")
        
        logger.debug(f"Loading product codes from {product_codes_path}")
        product_codes = pd.read_csv(product_codes_path)['product_code'].astype(str).tolist()
        logger.debug(f"Found {len(product_codes)} product codes")
        
        # Generate series IDs
        series_ids = [f"PCU{industry}{product}" for industry, product in itertools.product(industry_codes, product_codes)]
        logger.debug(f"Generated {len(series_ids)} series IDs")
        return series_ids
    
    def validate_series_batch(self, series_batch: List[str], retry_count: int = 0) -> Dict[str, bool]:
        """Validate a batch of series IDs against the BLS API with retries."""
        if not self.api_tracker.can_make_request():
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
            "registrationkey": self.api_key
        }
        
        try:
            response = self.session.post(BLS_API_URL, json=data, headers=headers)
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
                        
                self.api_tracker.increment()
                logger.info(f"Remaining API requests today: {self.api_tracker.remaining()}")
            else:
                error_message = result.get('message', 'Unknown error')
                logger.warning(f"API request failed: {error_message}")
                logger.debug(f"Full API response: {result}")
                
                # Check for API limit message after logging the error
                if re.search(r'daily\s+threshold|limit|' + re.escape(self.api_key), error_message, re.IGNORECASE):
                    logger.warning("Daily API limit reached. Stopping validation.")
                    validation_results = {}
                    
                if retry_count < MAX_RETRIES:
                    logger.info(f"Retrying batch (attempt {retry_count + 1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
                    return self.validate_series_batch(series_batch, retry_count + 1)
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
                return self.validate_series_batch(series_batch, retry_count + 1)
            else:
                logger.error(f"Max retries reached for batch. Marking as invalid.")
                return {series_id: False for series_id in series_batch}

    def validate_all_series(self, series_ids: List[str]) -> Dict[str, bool]:
        """Validate all series IDs using batched requests."""
        all_results = {}
        start_time = time.time()
        
        for i in range(0, len(series_ids), BATCH_SIZE):
            if not self.api_tracker.can_make_request():
                logger.warning("Daily API limit reached. Stopping validation.")
                break
                
            batch = series_ids[i:i + BATCH_SIZE]
            batch_start = time.time()
            logger.info(f"Validating batch {i//BATCH_SIZE + 1}/{(len(series_ids) + BATCH_SIZE - 1)//BATCH_SIZE}")
            
            batch_results = self.validate_series_batch(batch)
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
    
    def save_ppi_series_ids(self, series_ids: List[str], output_path: Path):
        """Save PPI series IDs to a file."""
        logger.debug(f"Saving {len(series_ids)} series IDs to {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            for series_id in series_ids:
                f.write(f"{series_id}\n")
        logger.debug("Series IDs saved successfully")
    
    def get_series_metadata(self, series_id: str) -> Dict[str, Any]:
        """Fetch metadata for a specific series."""
        logger.debug(f"Fetching metadata for series {series_id}")
        endpoint = f"{self.BASE_URL}/timeseries/metadata"
        params = {
            "seriesid": series_id,
            "api_key": self.api_key
        }
        
        response = self.session.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_series_data(self, series_ids: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """Fetch time series data for multiple series."""
        logger.debug(f"Fetching data for {len(series_ids)} series from {start_year} to {end_year}")
        endpoint = f"{self.BASE_URL}/timeseries/data"
        
        # BLS API has rate limits, so we'll add a delay
        time.sleep(1)
        
        payload = {
            "seriesid": series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
            "api_key": self.api_key
        }
        
        response = self.session.post(endpoint, json=payload)
        response.raise_for_status()
        
        data = response.json()
        if "Results" not in data:
            logger.error("No results found in API response")
            raise ValueError("No results found in API response")
            
        # Convert to DataFrame
        records = []
        for series in data["Results"]["series"]:
            series_id = series["seriesID"]
            for item in series["data"]:
                records.append({
                    "series_id": series_id,
                    "year": item["year"],
                    "period": item["period"],
                    "value": float(item["value"]),
                    "footnotes": item.get("footnotes", [])
                })
        
        df = pd.DataFrame(records)
        logger.debug(f"Retrieved {len(df)} data points")
        return df
    
    def extract(self, series_ids: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """Extract data from BLS API."""
        return self.get_series_data(series_ids, start_year, end_year)

class BLSSeriesExtractor:
    """Extractor for BLS series information from documentation."""
    
    def __init__(self):
        self.series_patterns = {
            'CPI': r'C[UW][0-9]{2}[0-9A-Z]{2}[0-9A-Z]',  # Consumer Price Index
            'PPI': r'PCU[0-9]{6}',  # Producer Price Index
            'CES': r'CE[0-9]{8}',  # Current Employment Statistics
            'JOLTS': r'JT[0-9]{8}',  # Job Openings and Labor Turnover Survey
            'LAUS': r'LA[0-9]{8}',  # Local Area Unemployment Statistics
            'OES': r'OE[0-9]{8}',  # Occupational Employment Statistics
            'QCEW': r'EN[0-9]{8}',  # Quarterly Census of Employment and Wages
        }
        
        self.series_components = {
            'CPI': {
                'prefix': ['CU', 'CW'],
                'seasonal': ['S', 'U'],
                'area': ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10'],
                'item': ['SA0', 'SA1', 'SA2', 'SA3', 'SA4', 'SA5', 'SA6', 'SA7', 'SA8', 'SA9'],
                'period': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']
            },
            'PPI': {
                'prefix': ['PCU'],
                'industry': [],  # Will be populated from PPI codes
                'product': [],   # Will be populated from PPI codes
                'period': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']
            }
        }

    def clean_text(self, text: str) -> str:
        """Clean up text by removing HTML and special characters."""
        # Remove HTML tags
        text = BeautifulSoup(text, 'html.parser').get_text()
        # Remove tabs and multiple spaces
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing whitespace
        text = text.strip()
        return text

    def extract_series_examples(self, page_source: str) -> Dict[str, List[str]]:
        """Extract series examples from the page source."""
        soup = BeautifulSoup(page_source, 'html.parser')
        examples = {}
        
        # Find all pre-formatted text blocks
        pre_blocks = soup.find_all('pre')
        for block in pre_blocks:
            text = block.get_text()
            for series_type, pattern in self.series_patterns.items():
                matches = re.finditer(pattern, text)
                if series_type not in examples:
                    examples[series_type] = set()
                for match in matches:
                    examples[series_type].add(match.group(0))
        
        # Convert sets to sorted lists
        return {k: sorted(list(v)) for k, v in examples.items()}

    def load_ppi_codes(self) -> Tuple[List[str], List[str]]:
        """Load PPI industry and product codes from CSV files."""
        bronze_dir = Path("data/bronze")
        
        industry_df = pd.read_csv(bronze_dir / "ppi_industry_codes.csv")
        product_df = pd.read_csv(bronze_dir / "ppi_product_codes.csv")
        
        return industry_df['code'].tolist(), product_df['code'].tolist()

    def generate_series_combinations(self) -> Dict[str, List[str]]:
        """Generate all possible series combinations."""
        # Load PPI codes
        industry_codes, product_codes = self.load_ppi_codes()
        self.series_components['PPI']['industry'] = industry_codes
        self.series_components['PPI']['product'] = product_codes
        
        combinations = {}
        
        # Generate CPI series
        cpi_series = []
        for prefix in self.series_components['CPI']['prefix']:
            for seasonal in self.series_components['CPI']['seasonal']:
                for area in self.series_components['CPI']['area']:
                    for item in self.series_components['CPI']['item']:
                        for period in self.series_components['CPI']['period']:
                            series_id = f"{prefix}{seasonal}{area}{item}{period}"
                            cpi_series.append(series_id)
        combinations['CPI'] = cpi_series
        
        # Generate PPI series
        ppi_series = []
        for prefix in self.series_components['PPI']['prefix']:
            for industry in self.series_components['PPI']['industry']:
                for product in self.series_components['PPI']['product']:
                    for period in self.series_components['PPI']['period']:
                        series_id = f"{prefix}{industry}{product}{period}"
                        ppi_series.append(series_id)
        combinations['PPI'] = ppi_series
        
        return combinations

    def download_and_extract(self):
        """Download BLS documentation and extract series information."""
        # Create directories
        bronze_dir = Path("data/bronze")
        bronze_dir.mkdir(parents=True, exist_ok=True)
        
        # BLS format documentation URL
        url = "https://www.bls.gov/help/hlpforma.htm"
        
        # Configure Chrome options
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36")
        
        try:
            logger.info("Setting up Chrome driver...")
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            logger.info(f"Accessing {url}...")
            driver.get(url)
            
            # Wait for the page to load
            time.sleep(5)
            
            # Get the page source
            page_source = driver.page_source
            
            # Extract series examples
            examples = self.extract_series_examples(page_source)
            
            # Generate all possible combinations
            combinations = self.generate_series_combinations()
            
            # Save results
            for series_type, series_list in combinations.items():
                df = pd.DataFrame({'series_id': series_list})
                output_path = bronze_dir / f"bls_{series_type.lower()}_series.csv"
                df.to_csv(output_path, index=False)
                logger.info(f"Saved {len(series_list)} {series_type} series to {output_path}")
                logger.info(f"Sample {series_type} series:\n{df.head()}")
            
            # Save examples
            examples_df = pd.DataFrame({
                'series_type': [k for k, v in examples.items() for _ in v],
                'series_id': [s for v in examples.values() for s in v]
            })
            examples_path = bronze_dir / "bls_series_examples.csv"
            examples_df.to_csv(examples_path, index=False)
            logger.info(f"Saved {len(examples_df)} example series to {examples_path}")
            
        except Exception as e:
            logger.error(f"Error extracting BLS series: {str(e)}")
            raise
        finally:
            if 'driver' in locals():
                driver.quit() 