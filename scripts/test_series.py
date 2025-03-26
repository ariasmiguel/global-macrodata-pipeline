import os
import requests
from dotenv import load_dotenv
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def test_series(series_id):
    """Test a specific series ID against the BLS API."""
    api_key = os.getenv('BLS_API_KEY')
    if not api_key:
        logger.error("BLS_API_KEY not found in environment variables")
        return

    # Get current year and previous year
    current_year = datetime.now().year
    previous_year = current_year - 1

    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    headers = {
        'BLS-API-KEY': api_key,
        'Content-Type': 'application/json'
    }
    
    data = {
        "seriesid": [series_id],
        "startyear": str(previous_year),
        "endyear": str(current_year),
        "registrationkey": api_key
    }

    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        result = response.json()

        if result.get('status') == 'REQUEST_SUCCEEDED':
            series_data = result.get('Results', {}).get('series', [])
            if series_data:
                logger.info(f"Series {series_id} is valid and active")
                logger.info(f"Latest data: {series_data[0].get('data', [])[0] if series_data[0].get('data') else 'No data'}")
            else:
                logger.warning(f"Series {series_id} returned no data")
        else:
            logger.error(f"API request failed: {result.get('message', 'Unknown error')}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request: {e}")

def main():
    # Test multiple series IDs
    series_ids = [
        "PCU22112222112241",  # Electric power distribution
        "PCU221122221122411", # New England residential electric power
        "PCU221122221122421", # New England commercial electric power
        "PCU221122221122431"  # New England industrial electric power
    ]

    for series_id in series_ids:
        logger.info(f"\nTesting series ID: {series_id}")
        test_series(series_id)

if __name__ == "__main__":
    main() 