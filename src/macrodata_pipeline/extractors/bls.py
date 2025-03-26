from typing import Any, Dict, List, Optional
import time

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