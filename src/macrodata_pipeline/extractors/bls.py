from typing import Any, Dict, List, Optional
import time
import itertools
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import os

from .base import BaseExtractor


class BLSExtractor(BaseExtractor):
    """Extractor for BLS API data."""
    
    BASE_URL = "https://api.bls.gov/publicAPI/v2"
    
    def __init__(self, api_key: Optional[str] = None):
        load_dotenv()
        self.api_key = api_key or os.getenv("BLS_API_KEY")
        if not self.api_key:
            raise ValueError("BLS API key is required")
        super().__init__()
    
    def generate_ppi_series_ids(self, industry_codes_path: Path, product_codes_path: Path) -> List[str]:
        """Generate PPI series IDs from industry and product codes."""
        # Load industry and product codes
        industry_codes = pd.read_csv(industry_codes_path)['industry_code'].astype(str).str.zfill(6).tolist()
        product_codes = pd.read_csv(product_codes_path)['product_code'].astype(str).tolist()
        
        # Generate series IDs
        series_ids = [f"PCU{industry}{product}" for industry, product in itertools.product(industry_codes, product_codes)]
        return series_ids
    
    def validate_ppi_series(self, series_ids: List[str], batch_size: int = 50) -> List[str]:
        """Validate PPI series IDs using the BLS API."""
        valid_series = []
        
        # Process in batches due to API limitations
        for i in range(0, len(series_ids), batch_size):
            batch = series_ids[i:i + batch_size]
            
            try:
                # Try to get data for the last year to validate
                df = self.get_series_data(
                    series_ids=batch,
                    start_year=2023,
                    end_year=2023
                )
                # If successful, add to valid series
                valid_series.extend(df['series_id'].unique())
            except Exception as e:
                print(f"Batch {i//batch_size} failed: {str(e)}")
            
            # Respect API rate limits
            time.sleep(1)
        
        return list(set(valid_series))
    
    def save_ppi_series_ids(self, series_ids: List[str], output_path: Path):
        """Save PPI series IDs to a file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            for series_id in series_ids:
                f.write(f"{series_id}\n")
    
    def get_series_metadata(self, series_id: str) -> Dict[str, Any]:
        """Fetch metadata for a specific series."""
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
        
        return pd.DataFrame(records)
    
    def extract(self, series_ids: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """Extract data from BLS API."""
        return self.get_series_data(series_ids, start_year, end_year) 