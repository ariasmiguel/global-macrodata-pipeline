import logging
from pathlib import Path
from macrodata_pipeline.extractors.bls import BLSExtractor
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_series(series_id: str, extractor: BLSExtractor):
    """Test a specific series ID against the BLS API."""
    try:
        # Get current year and previous year
        current_year = datetime.now().year
        previous_year = current_year - 1

        # Get series data
        df = extractor.get_series_data(
            series_ids=[series_id],
            start_year=previous_year,
            end_year=current_year
        )

        if not df.empty:
            logger.info(f"Series {series_id} is valid and active")
            logger.info(f"Latest data: {df.iloc[0].to_dict()}")
        else:
            logger.warning(f"Series {series_id} returned no data")

    except Exception as e:
        logger.error(f"Error testing series {series_id}: {str(e)}")

def main():
    # Initialize extractor
    extractor = BLSExtractor()
    
    # Test multiple series IDs
    series_ids = [
        "PCU22112222112241",  # Electric power distribution
        "PCU221122221122411", # New England residential electric power
        "PCU221122221122421", # New England commercial electric power
        "PCU221122221122431"  # New England industrial electric power
    ]

    for series_id in series_ids:
        logger.info(f"\nTesting series ID: {series_id}")
        test_series(series_id, extractor)

if __name__ == "__main__":
    main() 