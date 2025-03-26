import logging
from pathlib import Path
from macrodata_pipeline.extractors.bls import BLSSeriesExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to extract BLS series information."""
    try:
        extractor = BLSSeriesExtractor()
        extractor.download_and_extract()
    except Exception as e:
        logger.error(f"Error extracting BLS series: {str(e)}")
        raise

if __name__ == "__main__":
    main() 