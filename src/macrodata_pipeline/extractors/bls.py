from typing import Any, Dict, List, Optional, Tuple, Union
import time
import itertools
import logging
from pathlib import Path
import json
import re
from datetime import datetime
import math

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
        self.counts = {}
        self.last_errors = {}
        self._load_count()
    
    def _load_count(self):
        try:
            with open(self.cache_file, 'r') as f:
                data = json.load(f)
                if data.get('date') == self.today:
                    self.counts = data.get('counts', {})
                    self.last_errors = data.get('last_errors', {})
        except (FileNotFoundError, json.JSONDecodeError):
            pass  # Keep default empty dicts
    
    def _save_count(self):
        with open(self.cache_file, 'w') as f:
            json.dump({
                'date': self.today,
                'counts': self.counts,
                'last_errors': self.last_errors
            }, f)
    
    def increment(self, api_key: str):
        if api_key not in self.counts:
            self.counts[api_key] = 0
        self.counts[api_key] += 1
        self._save_count()
        logger.debug(f"Incremented count for API key {api_key}: {self.counts[api_key]}")
        
    def can_make_request(self, api_key: str = None):
        if api_key:
            return self.counts.get(api_key, 0) < API_LIMIT
        return any(self.counts.get(key, 0) < API_LIMIT for key in self.counts)
    
    def remaining(self, api_key: str = None):
        if api_key:
            return API_LIMIT - self.counts.get(api_key, 0)
        return {k: API_LIMIT - self.counts.get(k, 0) for k in self.counts}
    
    def set_error(self, api_key: str, error_message: str):
        self.last_errors[api_key] = error_message
        self._save_count()
        logger.debug(f"Set error for API key {api_key}: {error_message}")
    
    def get_status(self, api_key: str = None):
        if api_key:
            return {
                'count': self.counts.get(api_key, 0),
                'remaining': self.remaining(api_key),
                'limit': API_LIMIT,
                'last_error': self.last_errors.get(api_key)
            }
        return {
            'counts': self.counts,
            'remaining': self.remaining(),
            'limit': API_LIMIT,
            'last_errors': self.last_errors
        }
        
    def initialize_key(self, api_key: str):
        """Initialize tracking for a new API key."""
        if api_key not in self.counts:
            self.counts[api_key] = 0
            self._save_count()
            logger.debug(f"Initialized tracking for API key {api_key}")

class BLSExtractor(BaseExtractor):
    """Extractor for BLS API data."""
    
    BASE_URL = "https://api.bls.gov/publicAPI/v2"
    
    def __init__(self, api_keys: Optional[List[str]] = None):
        load_dotenv()
        self.api_keys = api_keys or [
            os.getenv("BLS_API_KEY"),
            os.getenv("BLS_API_KEY_2")
        ]
        self.api_keys = [k for k in self.api_keys if k]  # Remove empty keys
        
        if not self.api_keys:
            raise ValueError("At least one BLS API key is required")
            
        self.current_key_index = 0
        super().__init__()
        self.api_tracker = BLSAPITracker()
        
        # Initialize tracking for each API key
        for api_key in self.api_keys:
            self.api_tracker.initialize_key(api_key)
            
        logger.debug(f"Initialized BLSExtractor with {len(self.api_keys)} API keys")
        logger.debug(f"Initial API status: {self.api_tracker.get_status()}")
    
    @property
    def current_api_key(self):
        return self.api_keys[self.current_key_index]
    
    def rotate_api_key(self):
        """Rotate to the next available API key if possible."""
        next_index = (self.current_key_index + 1) % len(self.api_keys)
        if next_index != self.current_key_index:
            self.current_key_index = next_index
            logger.info("Rotated to next API key")
            return True
        return False
    
    def get_available_api_key(self):
        """Find an API key that hasn't reached its limit."""
        initial_key = self.current_api_key
        
        while True:
            if self.api_tracker.can_make_request(self.current_api_key):
                return self.current_api_key
                
            if not self.rotate_api_key() or self.current_api_key == initial_key:
                logger.warning("All API keys have reached their daily limit")
                return None
    
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
        api_key = self.get_available_api_key()
        if not api_key:
            status = self.api_tracker.get_status()
            logger.warning(f"All API keys have reached their daily limit. Status: {status}")
            return {series_id: False for series_id in series_batch}
        
        headers = {'Content-type': 'application/json'}
        
        # Check current and previous year only
        current_year = datetime.now().year
        years = [str(current_year - i) for i in range(2)]  # Check last 2 years
        
        data = {
            "seriesid": series_batch,
            "startyear": years[-1],  # Use the older year as start
            "endyear": years[0],     # Use the newer year as end
            "registrationkey": api_key
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
                    validation_results[series_id] = True
                    logger.debug(f"Series {series_id} found in API response")
                
                # Mark missing series as invalid
                for series_id in series_batch:
                    if series_id not in validation_results:
                        validation_results[series_id] = False
                        logger.debug(f"Series {series_id} not found in API response")
                        
                self.api_tracker.increment(api_key)
                status = self.api_tracker.get_status(api_key)
                logger.info(f"API Status: {status}")
            else:
                error_message = result.get('message', 'Unknown error')
                self.api_tracker.set_error(api_key, error_message)
                logger.warning(f"API request failed: {error_message}")
                logger.debug(f"Full API response: {result}")
                
                if re.search(r'daily\s+threshold|limit|' + re.escape(api_key), error_message, re.IGNORECASE):
                    logger.warning("Daily API limit reached. Stopping validation.")
                    validation_results = {}
                    
                if retry_count < MAX_RETRIES:
                    logger.info(f"Retrying batch (attempt {retry_count + 1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
                    return self.validate_series_batch(series_batch, retry_count + 1)
                else:
                    logger.error(f"Max retries reached for batch. Marking as invalid.")
                    validation_results = {}
            
            return {series_id: bool(is_valid) for series_id, is_valid in validation_results.items()}
        
        except Exception as e:
            error_msg = str(e)
            self.api_tracker.set_error(api_key, error_msg)
            logger.error(f"Error validating batch: {error_msg}")
            if retry_count < MAX_RETRIES:
                logger.info(f"Retrying batch (attempt {retry_count + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
                return self.validate_series_batch(series_batch, retry_count + 1)
            else:
                logger.error(f"Max retries reached for batch. Marking as invalid.")
                return {series_id: False for series_id in series_batch}

    def validate_all_series(self, series_ids: List[str], batch_size: int = 50) -> Dict[str, List[str]]:
        """Validate a list of series IDs in batches."""
        start_time = time.time()
        valid_series = []
        invalid_series = []
        
        for i in range(0, len(series_ids), batch_size):
            batch = series_ids[i:i + batch_size]
            api_key = self.get_available_api_key()
            
            if not api_key:
                status = self.api_tracker.get_status()
                logger.warning(f"All API keys have reached their daily limit. Status: {status}")
                break
                
            try:
                logger.info(f"Validating batch {i//batch_size + 1} of {math.ceil(len(series_ids)/batch_size)}")
                data = self.get_series_data(batch, 2023, 2023, raw_output=True)
                
                # Process results
                for series in data.get("Results", {}).get("series", []):
                    valid_series.append(series["seriesID"])
                
                # Find invalid series by comparing with the batch
                batch_invalid = [s for s in batch if s not in valid_series]
                invalid_series.extend(batch_invalid)
                
            except Exception as e:
                logger.error(f"Error validating batch: {str(e)}")
                invalid_series.extend(batch)
                if "daily threshold" in str(e).lower():
                    if not self.rotate_api_key():
                        break
        
        total_time = time.time() - start_time
        logger.info(f"Validation completed in {total_time:.2f} seconds")
        logger.info(f"API Status: {self.api_tracker.get_status()}")
        
        return {
            "valid": valid_series,
            "invalid": invalid_series
        }
    
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
        api_key = self.get_available_api_key()
        if not api_key:
            status = self.api_tracker.get_status()
            logger.warning(f"All API keys have reached their daily limit. Status: {status}")
            raise ValueError(f"All API keys have reached their daily limit. Status: {status}")
            
        logger.debug(f"Fetching metadata for series {series_id}")
        endpoint = f"{self.BASE_URL}/timeseries/metadata"
        params = {
            "seriesid": series_id,
            "registrationkey": api_key
        }
        
        try:
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'REQUEST_NOT_PROCESSED':
                error_msg = data.get('message', ['Unknown error'])[0]
                self.api_tracker.set_error(api_key, error_msg)
                if "daily threshold" in error_msg.lower():
                    # Try with another API key
                    if self.rotate_api_key():
                        return self.get_series_metadata(series_id)
                raise ValueError(error_msg)
            
            self.api_tracker.increment(api_key)
            status = self.api_tracker.get_status(api_key)
            logger.info(f"API Status: {status}")
            
            return data
        except Exception as e:
            error_msg = str(e)
            self.api_tracker.set_error(api_key, error_msg)
            logger.error(f"Error fetching metadata: {error_msg}")
            raise
    
    def get_series_data(self, series_ids: List[str], start_year: int, end_year: int, raw_output: bool = False) -> Union[pd.DataFrame, Dict]:
        """Fetch time series data for multiple series."""
        api_key = self.get_available_api_key()
        if not api_key:
            status = self.api_tracker.get_status()
            logger.warning(f"All API keys have reached their daily limit. Status: {status}")
            raise ValueError(f"All API keys have reached their daily limit. Status: {status}")
            
        logger.debug(f"Fetching data for {len(series_ids)} series from {start_year} to {end_year}")
        endpoint = f"{self.BASE_URL}/timeseries/data"
        
        time.sleep(1)
        
        payload = {
            "seriesid": series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
            "registrationkey": api_key
        }
        
        try:
            response = self.session.post(endpoint, json=payload)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'REQUEST_NOT_PROCESSED':
                error_msg = data.get('message', ['Unknown error'])[0]
                self.api_tracker.set_error(api_key, error_msg)
                if "daily threshold" in error_msg.lower():
                    # Try with another API key
                    if self.rotate_api_key():
                        return self.get_series_data(series_ids, start_year, end_year, raw_output)
                raise ValueError(error_msg)
            
            if "Results" not in data:
                error_msg = "No results found in API response"
                self.api_tracker.set_error(api_key, error_msg)
                logger.error(error_msg)
                raise ValueError(error_msg)
                
            self.api_tracker.increment(api_key)
            status = self.api_tracker.get_status(api_key)
            logger.info(f"API Status: {status}")
            
            if raw_output:
                return data
                
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
            
        except Exception as e:
            error_msg = str(e)
            self.api_tracker.set_error(api_key, error_msg)
            logger.error(f"Error fetching series data: {error_msg}")
            raise
    
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