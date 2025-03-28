"""Data processing utilities for the Macroeconomic Data Pipeline.

This module provides utilities for processing and cleaning data, including
memory management, parallel processing, and data standardization.
"""

import os
import psutil
import logging
from pathlib import Path
from typing import Dict, List, Any, Tuple
import pandas as pd
import json
import concurrent.futures
from datetime import datetime

from macrodata_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

# Performance settings
MAX_WORKERS = os.cpu_count() or 4
CHUNK_SIZE = 100000  # Number of rows to process at once

def log_memory_usage() -> None:
    """Log current memory usage of the process."""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    logger.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")

def process_json_file(file_path: Path) -> List[Tuple[str, List[Dict[str, Any]]]]:
    """Process a single JSON file and extract series data.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        List of tuples containing (series_id, data_points)
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
            # Handle consolidated data structure
            if isinstance(data, dict):
                results = []
                for series_id, series_info in data.items():
                    if isinstance(series_info, dict) and 'data' in series_info:
                        # Extract the data points from the series
                        series_data = series_info['data'].get('data', [])
                        if series_data:
                            results.append((series_id, series_data))
                return results
                
            # Handle individual series structure
            if "seriesID" in data:
                series_id = data["seriesID"]
                return [(series_id, data.get("data", []))]
            elif "series_id" in data:
                series_id = data["series_id"]
                return [(series_id, data.get("data", []))]
            else:
                # Try to determine series_id from filename
                series_id = file_path.stem.split("_")[-1]
                # Find any list in the data that might contain time series points
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0:
                        return [(series_id, value)]
    except Exception as e:
        logger.error(f"Error loading {file_path}: {str(e)}")
    return []

def load_json_files_parallel(directory: Path) -> Dict[str, List[Dict[str, Any]]]:
    """Load JSON files from a directory using parallel processing.
    
    Args:
        directory: Directory containing JSON files
        
    Returns:
        Dictionary mapping series IDs to their data points
    """
    if not directory.exists():
        logger.error(f"Directory not found: {directory}")
        return {}
        
    series_data = {}
    start_time = datetime.now()
    
    # Get list of files to process
    files = [f for f in directory.glob("*.json") 
             if f.name not in ["extraction_results.json", "extraction_progress.json"]]
    
    if not files:
        logger.warning(f"No JSON files found in {directory}")
        return series_data
    
    logger.info(f"Found {len(files)} JSON files to process")
    
    # Process files in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(process_json_file, files))
    
    # Collect results
    for file_results in results:
        for series_id, data in file_results:
            if series_id and data:
                series_data[series_id] = data
    
    logger.info(f"Processed {len(files)} files in {(datetime.now() - start_time).total_seconds():.2f} seconds")
    log_memory_usage()
    
    return series_data

def process_data_chunk(
    data_chunk: List[Tuple[str, List[Dict[str, Any]]]],
    extraction_timestamp: pd.Timestamp
) -> pd.DataFrame:
    """Process a chunk of data points into a standardized DataFrame.
    
    Args:
        data_chunk: List of (series_id, data_points) tuples
        extraction_timestamp: Timestamp when the data was extracted
        
    Returns:
        DataFrame containing processed data points
    """
    records = []
    
    for series_id, data_points in data_chunk:
        for point in data_points:
            try:
                record = {
                    'series_id': series_id,
                    'year': int(point.get('year', 0)),
                    'period': point.get('period', ''),
                    'value': float(point.get('value', 0)) if point.get('value', '-') != '-' else None,
                    'footnotes': json.dumps(point.get('footnotes', [])),
                    'extraction_timestamp': extraction_timestamp
                }
                # Convert M13 to M12 for annual averages
                if record['period'] == 'M13':
                    record['period'] = 'M12'
                records.append(record)
            except (ValueError, TypeError) as e:
                logger.warning(f"Error processing data point for series {series_id}: {str(e)}")
                continue
    
    return pd.DataFrame(records)

def standardize_series(series_data: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
    """Standardize series data into a DataFrame format.
    
    Args:
        series_data: Dictionary mapping series IDs to their data points
        
    Returns:
        DataFrame containing standardized series data
    """
    logger.info("Standardizing series data")
    
    try:
        # Process data in chunks to manage memory
        all_records = []
        extraction_timestamp = pd.Timestamp.now(tz='UTC')
        
        # Convert dictionary items to list for chunking
        items = list(series_data.items())
        
        for i in range(0, len(items), CHUNK_SIZE):
            chunk = items[i:i + CHUNK_SIZE]
            chunk_df = process_data_chunk(chunk, extraction_timestamp)
            all_records.append(chunk_df)
            
            # Log progress
            if (i + CHUNK_SIZE) % (CHUNK_SIZE * 10) == 0:
                logger.info(f"Processed {i + CHUNK_SIZE} series")
                log_memory_usage()
        
        # Combine all chunks
        df = pd.concat(all_records, ignore_index=True)
        
        # Convert date columns
        df['date'] = pd.to_datetime(
            df['year'].astype(str) + '-' + df['period'].str[1:],
            format='%Y-%m'
        )
        
        # Sort by series_id and date
        df = df.sort_values(['series_id', 'date'])
        
        logger.info(f"Standardized {len(df)} data points")
        return df
        
    except Exception as e:
        logger.error(f"Error standardizing series data: {str(e)}")
        raise 