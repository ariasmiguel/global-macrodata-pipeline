from typing import Any, Dict, List, Optional, Tuple, Union
import time
import logging
from pathlib import Path
import json
import re
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
import os

from .base import BaseExtractor

# Configure logging
logger = logging.getLogger(__name__)

# BLS API Configuration
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BATCH_SIZE = 50  # BLS allows up to 50 series per request
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
API_LIMIT = 500  # Daily API limit
SLEEP_BETWEEN_BATCHES = 0.75  # Increased to 750ms to be more conservative

class BLSAPITracker:
    def __init__(self, cache_file=None):
        if cache_file is None:
            # Create data directory if it doesn't exist
            data_dir = Path("data")
            data_dir.mkdir(parents=True, exist_ok=True)
            cache_file = data_dir / '.bls_api_count.json'
        self.cache_file = Path(cache_file)
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
                else:
                    # Reset counts if it's a new day
                    self.counts = {}
                    self.last_errors = {}
        except (FileNotFoundError, json.JSONDecodeError):
            pass  # Keep default empty dicts
    
    def _save_count(self):
        # Ensure parent directory exists
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
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
        """Check if we can make a request with the given API key or any available key."""
        if api_key:
            # If no count exists for the key, we can make a request
            if api_key not in self.counts:
                return True
            return self.counts[api_key] < API_LIMIT
        
        # If no counts exist at all, we can make a request
        if not self.counts:
            return True
            
        # Check if any key has remaining requests
        return any(self.counts.get(key, 0) < API_LIMIT for key in self.counts)
    
    def get_status(self, api_key: str = None):
        """Get the current status of API usage."""
        if api_key:
            count = self.counts.get(api_key, 0)
            return {
                'count': count,
                'remaining': API_LIMIT - count,
                'limit': API_LIMIT,
                'last_error': self.last_errors.get(api_key)
            }
        
        # If no counts exist, return empty status
        if not self.counts:
            return {
                'counts': {},
                'remaining': {},
                'limit': API_LIMIT,
                'last_errors': {}
            }
            
        return {
            'counts': self.counts,
            'remaining': {k: API_LIMIT - self.counts.get(k, 0) for k in self.counts},
            'limit': API_LIMIT,
            'last_errors': self.last_errors
        }
        
    def set_error(self, api_key: str, error_message: str):
        self.last_errors[api_key] = error_message
        self._save_count()
        logger.debug(f"Set error for API key {api_key}: {error_message}")

class BLSExtractor(BaseExtractor):
    """Extractor for BLS API data."""
    
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
            
        logger.debug(f"Initialized BLSExtractor with {len(self.api_keys)} API keys")
        logger.debug(f"Initial API status: {self.api_tracker.get_status()}")
    
    def extract(self) -> Any:
        """Extract data from BLS API. This is a placeholder implementation of the abstract method."""
        pass
        
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
    
    def get_series_data(self, series_id: Union[str, List[str]], start_year: Union[str, int], end_year: Union[str, int], raw_output: bool = False) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Get historical data for one or more series."""
        api_key = self.get_available_api_key()
        if not api_key:
            status = self.api_tracker.get_status()
            logger.warning(f"All API keys have reached their daily limit. Status: {status}")
            raise Exception("All API keys have reached their daily limit")
        
        headers = {'Content-type': 'application/json'}
        series_ids = [series_id] if isinstance(series_id, str) else series_id
        
        # Validate input parameters
        if not series_ids:
            raise ValueError("No series IDs provided")
            
        if not all(isinstance(sid, str) for sid in series_ids):
            raise ValueError("All series IDs must be strings")
            
        # Convert years to strings and validate
        start_year_str = str(start_year)
        end_year_str = str(end_year)
            
        if not start_year_str.isdigit() or not end_year_str.isdigit():
            raise ValueError("Start and end years must be numeric")
            
        if int(start_year_str) > int(end_year_str):
            raise ValueError("Start year must be less than or equal to end year")
        
        data = {
            "seriesid": series_ids,
            "startyear": start_year_str,
            "endyear": end_year_str,
            "registrationkey": api_key
        }
        
        # Log request details
        logger.debug(f"Making BLS API request with parameters: {json.dumps(data, indent=2)}")
        
        try:
            response = self.session.post(BLS_API_URL, json=data, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            # Log response details
            logger.debug(f"BLS API response: {json.dumps(result, indent=2)}")
            
            if result.get('status') == 'REQUEST_SUCCEEDED':
                series_data = result.get('Results', {}).get('series', [])
                if series_data:
                    self.api_tracker.increment(api_key)
                    if raw_output:
                        return result
                    elif isinstance(series_id, str):
                        return series_data[0].get('data', [])
                    else:
                        return {s['seriesID']: s.get('data', []) for s in series_data}
                else:
                    error_msg = f"No data found for series {series_id}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                    
            elif result.get('status') == 'REQUEST_NOT_PROCESSED':
                error_message = result.get('message', 'Unknown error')
                self.api_tracker.set_error(api_key, error_message)
                
                if re.search(r'daily\s+threshold|limit|' + re.escape(api_key), error_message, re.IGNORECASE):
                    logger.warning("Daily API limit reached. Rotating API key...")
                    if self.rotate_api_key():
                        return self.get_series_data(series_id, start_year, end_year, raw_output)
                    else:
                        raise Exception("All API keys have reached their daily limit")
                else:
                    error_msg = f"Request not processed: {error_message}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                    
            else:
                error_message = result.get('message', 'Unknown error')
                self.api_tracker.set_error(api_key, error_message)
                error_msg = f"API request failed: {error_message}"
                logger.error(error_msg)
                raise Exception(error_msg)
                
        except Exception as e:
            self.api_tracker.set_error(api_key, str(e))
            logger.error(f"Exception during API request: {str(e)}")
            logger.error(f"Request parameters: {json.dumps(data, indent=2)}")
            raise
    
    def validate_all_series(self, series_ids: List[str], batch_size: int = 50) -> Dict[str, bool]:
        """Validate all series IDs against the BLS API in batches."""
        validation_results = {}
        
        for i in range(0, len(series_ids), batch_size):
            batch = series_ids[i:i + batch_size]
            logger.info(f"Validating batch {i//batch_size + 1} of {(len(series_ids) + batch_size - 1)//batch_size}")
            
            api_key = self.get_available_api_key()
            if not api_key:
                status = self.api_tracker.get_status()
                logger.warning(f"All API keys have reached their daily limit. Status: {status}")
                # Mark remaining series as invalid
                for series_id in series_ids[i:]:
                    validation_results[series_id] = False
                break
            
            try:
                # Get current and previous year only for validation
                current_year = datetime.now().year
                previous_year = current_year - 1
                
                data = self.get_series_data(batch, str(previous_year), str(current_year), raw_output=True)
                
                if data.get('status') == 'REQUEST_SUCCEEDED':
                    series_data = data.get('Results', {}).get('series', [])
                    for series in series_data:
                        series_id = series['seriesID']
                        validation_results[series_id] = bool(series.get('data'))
                        
                    # Mark missing series as invalid
                    for series_id in batch:
                        if series_id not in validation_results:
                            validation_results[series_id] = False
                else:
                    # Mark all series in batch as invalid if request failed
                    for series_id in batch:
                        validation_results[series_id] = False
                        
            except Exception as e:
                logger.error(f"Error validating batch: {str(e)}")
                for series_id in batch:
                    validation_results[series_id] = False
                
            # Add a small delay between batches
            time.sleep(SLEEP_BETWEEN_BATCHES)
        
        return validation_results 