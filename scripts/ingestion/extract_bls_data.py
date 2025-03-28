import logging
from pathlib import Path
import pandas as pd
import time
from datetime import datetime
import json
from typing import List, Dict
import os

from macrodata_pipeline.extractors.bls import BLSExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bls_extraction.log')
    ]
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set logger to DEBUG level to see all messages

BATCH_SIZE = 50
START_YEAR = 2010
END_YEAR = datetime.now().year

def get_api_keys() -> List[str]:
    """Get all available BLS API keys from environment variables."""
    api_keys = []
    if os.getenv("BLS_API_KEY"):
        api_keys.append(os.getenv("BLS_API_KEY"))
    if os.getenv("BLS_API_KEY_2"):
        api_keys.append(os.getenv("BLS_API_KEY_2"))
    if not api_keys:
        raise ValueError("No BLS API keys found in environment variables")
    return api_keys

def load_validated_series(series_type: str = None) -> pd.DataFrame:
    """Load validated PPI series, optionally filtered by type.
    
    Args:
        series_type: Optional filter for 'aggregated', 'industry', or 'commodity'
    """
    bronze_dir = Path("data/bronze")
    series_df = pd.read_csv(bronze_dir / "validated_ppi_series.csv")
    if series_type:
        return series_df[series_df['series_type'] == series_type]
    return series_df

def process_batch(series_batch: List[Dict], extractor: BLSExtractor, output_dir: Path) -> Dict:
    """Process a batch of series IDs and save raw JSON output."""
    batch_results = {}
    
    try:
        # Check API status before processing
        api_status = extractor.api_tracker.get_status()
        logger.info(f"Current API Status: {api_status}")
        
        if not extractor.api_tracker.can_make_request():
            logger.warning(f"API limit reached. Status: {api_status}")
            for series in series_batch:
                batch_results[series['series_id']] = {
                    'status': 'error',
                    'error': f"API limit reached: {api_status}",
                    'series_type': series['series_type']
                }
            return batch_results
        
        # Get historical data for the batch
        series_ids = [s['series_id'] for s in series_batch]
        data = extractor.get_series_data(series_ids, START_YEAR, END_YEAR, raw_output=True)
        
        # Log the raw response for debugging
        logger.debug(f"Raw API response: {json.dumps(data, indent=2)}")
        
        # Store raw JSON for each series
        if "Results" in data and "series" in data["Results"]:
            for series in data["Results"]["series"]:
                series_id = series["seriesID"]
                # Find series type from batch data
                series_type = next(s['series_type'] for s in series_batch if s['series_id'] == series_id)
                output_file = output_dir / f"{series_type}_{series_id}.json"
                
                with open(output_file, 'w') as f:
                    json.dump(series, f, indent=2)
                
                batch_results[series_id] = {
                    'status': 'success',
                    'file_path': str(output_file),
                    'series_type': series_type
                }
        else:
            # Mark all series in batch as failed if no data returned
            error_msg = f"No data found in API response. Response: {json.dumps(data, indent=2)}"
            logger.error(error_msg)
            for series in series_batch:
                series_id = series['series_id']
                batch_results[series_id] = {
                    'status': 'error',
                    'error': error_msg,
                    'series_type': series['series_type']
                }
            
        # Check for missing series in response
        received_series = set(batch_results.keys())
        expected_series = set(s['series_id'] for s in series_batch)
        missing_series = expected_series - received_series
        
        if missing_series:
            logger.warning(f"Missing series in response: {missing_series}")
        
        # Mark missing series as failed
        for series in series_batch:
            if series['series_id'] in missing_series:
                batch_results[series['series_id']] = {
                    'status': 'error',
                    'error': 'Series not found in API response',
                    'series_type': series['series_type']
                }
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error processing batch: {error_msg}")
        logger.error(f"Full error details: {e.__class__.__name__}: {error_msg}")
        for series in series_batch:
            batch_results[series['series_id']] = {
                'status': 'error',
                'error': error_msg,
                'series_type': series['series_type']
            }
    
    # Log detailed batch statistics by type
    stats = {}
    for result in batch_results.values():
        series_type = result['series_type']
        if series_type not in stats:
            stats[series_type] = {'success': 0, 'error': 0, 'total': 0}
        stats[series_type]['total'] += 1
        stats[series_type][result['status']] += 1
    
    for series_type, counts in stats.items():
        logger.info(f"Batch details for {series_type}: "
                   f"{counts['success']}/{counts['total']} successful, "
                   f"{counts['error']}/{counts['total']} failed")
        
        # Log error details for failed series
        if counts['error'] > 0:
            error_series = [s['series_id'] for s in series_batch 
                          if batch_results[s['series_id']]['status'] == 'error']
            logger.error(f"Failed series IDs: {error_series}")
            for series_id in error_series:
                logger.error(f"Error for {series_id}: {batch_results[series_id]['error']}")
    
    return batch_results

def extract_series_data(series_df: pd.DataFrame, extractor: BLSExtractor, max_batches: int = None) -> dict:
    """Extract historical data for all series and store raw JSON output."""
    # Create output directory
    output_dir = Path("data/bronze/raw_series_data")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load previous progress if exists
    progress_file = output_dir / "extraction_progress.json"
    if progress_file.exists():
        with open(progress_file, 'r') as f:
            results = json.load(f)
        # Get processed series IDs
        processed_series = set(results.keys())
        # Filter out already processed series
        series_df = series_df[~series_df['series_id'].isin(processed_series)]
        logger.info(f"Loaded {len(processed_series)} previously processed series")
    else:
        results = {}
    
    series_list = series_df.to_dict('records')
    total_batches = (len(series_list) + BATCH_SIZE - 1) // BATCH_SIZE
    if max_batches:
        total_batches = min(total_batches, max_batches)
    
    try:
        # Process series in batches
        for i in range(0, len(series_list), BATCH_SIZE):
            batch = series_list[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            
            if max_batches and batch_num > max_batches:
                logger.info(f"Reached maximum number of batches ({max_batches})")
                break
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} series)")
            
            # Check API status before processing batch
            api_status = extractor.api_tracker.get_status()
            if not extractor.api_tracker.can_make_request():
                logger.warning(f"API daily limit reached. Status: {api_status}")
                logger.info("Saving progress and stopping...")
                break
            
            # Process the batch
            batch_results = process_batch(batch, extractor, output_dir)
            
            # Check if batch failed due to API limit
            any_api_limit = any("daily threshold" in result.get('error', '').lower() 
                              for result in batch_results.values())
            
            if any_api_limit:
                logger.warning("API limit reached during batch processing")
                logger.info("Saving progress and stopping...")
                results.update(batch_results)
                break
            
            results.update(batch_results)
            
            # Save progress after each batch
            with open(progress_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            # Add delay between batches if not stopping
            if batch_num < total_batches and not any_api_limit:
                time.sleep(1)
    
    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}")
        # Save progress on error
        with open(progress_file, 'w') as f:
            json.dump(results, f, indent=2)
        raise
    
    return results

def main():
    """Main execution function."""
    start_time = time.time()
    
    # Initialize BLS extractor with all available API keys
    api_keys = get_api_keys()
    logger.info(f"Found {len(api_keys)} API keys")
    extractor = BLSExtractor(api_keys=api_keys)
    
    # Process each type of series
    for series_type in ['aggregated', 'industry', 'commodity']:
        logger.info(f"\nProcessing {series_type} series...")
        series_df = load_validated_series(series_type)
        logger.info(f"Found {len(series_df)} {series_type} series to process")
        logger.info(f"Will process in batches of {BATCH_SIZE} series")
        logger.info(f"Data range: {START_YEAR} to {END_YEAR}")
        logger.info("Extracting historical data...")
        
        results = extract_series_data(series_df, extractor)
        
        # Log summary statistics
        success_count = sum(1 for r in results.values() if r['status'] == 'success')
        error_count = sum(1 for r in results.values() if r['status'] == 'error')
        
        logger.info(f"\nExtraction Summary for {series_type}:")
        logger.info(f"Total series processed: {len(results)}")
        logger.info(f"Successful extractions: {success_count}")
        logger.info(f"Failed extractions: {error_count}")
    
    # Save final results
    output_dir = Path("data/bronze/raw_series_data")
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / "extraction_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    end_time = time.time()
    logger.info(f"\nTotal execution time: {end_time - start_time:.2f} seconds")
    logger.info(f"Results summary saved to {output_dir}/extraction_results.json")

if __name__ == "__main__":
    main() 