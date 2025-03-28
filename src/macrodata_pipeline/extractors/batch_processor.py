"""
Module for batch processing of BLS data extraction.
"""

import logging
from pathlib import Path
import pandas as pd
import time
from datetime import datetime
import json
from typing import List, Dict, Optional
import os

from .bls import BLSExtractor

logger = logging.getLogger(__name__)

class BLSBatchProcessor:
    """Handles batch processing of BLS data extraction."""
    
    def __init__(self, extractor: BLSExtractor, batch_size: int = 50):
        self.extractor = extractor
        self.batch_size = batch_size
        self.output_dir = Path("data/bronze/raw_series_data")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize consolidated data file if it doesn't exist
        self.consolidated_file = self.output_dir / "consolidated_data.json"
        if not self.consolidated_file.exists():
            with open(self.consolidated_file, 'w') as f:
                json.dump({}, f)
    
    def process_batch(self, series_batch: List[Dict], start_year: int, end_year: int) -> Dict:
        """Process a batch of series IDs and save raw JSON output."""
        batch_results = {}
        consolidated_data = {}
        
        try:
            # Check API status before processing
            api_status = self.extractor.api_tracker.get_status()
            logger.info(f"Current API Status: {api_status}")
            
            if not self.extractor.api_tracker.can_make_request():
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
            data = self.extractor.get_series_data(series_ids, start_year, end_year, raw_output=True)
            
            # Log the raw response for debugging
            logger.debug(f"Raw API response: {json.dumps(data, indent=2)}")
            
            # Store raw JSON for each series
            if "Results" in data and "series" in data["Results"]:
                for series in data["Results"]["series"]:
                    series_id = series["seriesID"]
                    # Find series type from batch data
                    series_type = next(s['series_type'] for s in series_batch if s['series_id'] == series_id)
                    
                    # Add to consolidated data
                    consolidated_data[series_id] = {
                        'series_type': series_type,
                        'data': series
                    }
                    
                    batch_results[series_id] = {
                        'status': 'success',
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
        
        # Save consolidated data to file
        if consolidated_data:
            if self.consolidated_file.exists():
                with open(self.consolidated_file, 'r') as f:
                    existing_data = json.load(f)
                existing_data.update(consolidated_data)
                consolidated_data = existing_data
            
            with open(self.consolidated_file, 'w') as f:
                json.dump(consolidated_data, f, indent=2)
        
        return batch_results
    
    def extract_series_data(self, series_df: pd.DataFrame, start_year: int, end_year: int, max_batches: Optional[int] = None) -> dict:
        """Extract historical data for all series and store raw JSON output."""
        # Load previous progress if exists
        progress_file = self.output_dir / "extraction_progress.json"
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
        total_batches = (len(series_list) + self.batch_size - 1) // self.batch_size
        if max_batches:
            total_batches = min(total_batches, max_batches)
        
        try:
            # Process series in batches
            for i in range(0, len(series_list), self.batch_size):
                batch = series_list[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                
                if max_batches and batch_num > max_batches:
                    logger.info(f"Reached maximum number of batches ({max_batches})")
                    break
                
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} series)")
                
                # Check API status before processing batch
                api_status = self.extractor.api_tracker.get_status()
                if not self.extractor.api_tracker.can_make_request():
                    logger.warning(f"API daily limit reached. Status: {api_status}")
                    logger.info("Saving progress and stopping...")
                    break
                
                # Process the batch
                batch_results = self.process_batch(batch, start_year, end_year)
                
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