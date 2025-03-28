import logging
from pathlib import Path
import pandas as pd
import json
from typing import Dict, List, Tuple
import pyarrow as pa
import pyarrow.parquet as pq
import time
import re
import concurrent.futures
from functools import partial
import os
import psutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bls_cleaning.log')
    ]
)
logger = logging.getLogger(__name__)

# Performance settings
MAX_WORKERS = os.cpu_count() or 4
CHUNK_SIZE = 100000  # Number of rows to process at once

def log_memory_usage():
    """Log current memory usage of the process."""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    logger.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")

def process_file(file_path: Path) -> Tuple[str, List]:
    """Process a single JSON file and extract series data."""
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

def load_raw_series_data(bronze_dir: Path) -> Dict[str, List]:
    """Load raw JSON series data from bronze layer using parallel processing."""
    raw_data_dir = bronze_dir / "raw_series_data"
    if not raw_data_dir.exists():
        logger.error(f"Directory not found: {raw_data_dir}")
        return {}
        
    series_data = {}
    start_time = time.time()
    
    # Get list of files to process
    files = [f for f in raw_data_dir.glob("*.json") 
             if f.name not in ["extraction_results.json", "extraction_progress.json"]]
    
    if not files:
        logger.warning(f"No JSON files found in {raw_data_dir}")
        return series_data
    
    logger.info(f"Found {len(files)} JSON files to process")
    
    # Process files in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(process_file, files))
    
    # Collect results
    for file_results in results:
        for series_id, data in file_results:
            if series_id and data:
                series_data[series_id] = data
    
    logger.info(f"Processed {len(files)} files in {time.time() - start_time:.2f} seconds")
    log_memory_usage()
    
    # If no data found, try subdirectories
    if not series_data:
        logger.info("No data found in main directory, checking subdirectories...")
        
        # Find all subdirectories
        subdirs = [d for d in raw_data_dir.glob("*") if d.is_dir()]
        
        for subdir in subdirs:
            subdir_files = list(subdir.glob("*.json"))
            logger.info(f"Found {len(subdir_files)} files in {subdir}")
            
            # Process files in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                results = list(executor.map(process_file, subdir_files))
            
            # Collect results
            for file_results in results:
                for series_id, data in file_results:
                    if series_id and data:
                        series_data[series_id] = data
    
    return series_data

def process_validated_series(bronze_dir: Path, silver_dir: Path):
    """Process the validated PPI series CSV file and save as Parquet."""
    start_time = time.time()
    validated_file = bronze_dir / "validated_ppi_series.csv"
    
    if not validated_file.exists():
        logger.warning(f"Validated PPI series file not found: {validated_file}")
        return
    
    logger.info("Processing validated PPI series...")
    
    try:
        # Read the CSV file
        df = pd.read_csv(validated_file)
        
        # Basic validation and cleaning
        logger.info(f"Loaded {len(df)} validated series")
        
        # Convert boolean columns properly
        if 'is_valid' in df.columns:
            df['is_valid'] = df['is_valid'].astype(bool)
        
        # Add timestamp for when this was processed
        df['processed_timestamp'] = pd.Timestamp.now(tz='UTC')
        
        # Convert to PyArrow table
        schema = []
        for col in df.columns:
            if col == 'series_id' or col == 'series_name' or col == 'series_type':
                schema.append((col, pa.string()))
            elif col == 'is_valid':
                schema.append((col, pa.bool_()))
            elif col == 'processed_timestamp':
                schema.append((col, pa.timestamp('ns', tz='UTC')))
            else:
                # Default to string for other columns
                schema.append((col, pa.string()))
        
        table = pa.Table.from_pandas(df, schema=pa.schema(schema))
        
        # Save to Parquet
        output_path = silver_dir / 'series_metadata.parquet'
        pq.write_table(table, output_path, compression='snappy')
        
        logger.info(f"Saved validated series to {output_path}")
        logger.info(f"Processing completed in {time.time() - start_time:.2f} seconds")
        log_memory_usage()
    
    except Exception as e:
        logger.error(f"Error processing validated series: {str(e)}")

def process_data_chunk(data_chunk: List[Dict], extraction_timestamp: pd.Timestamp) -> pd.DataFrame:
    """Process a chunk of data points into a standardized DataFrame."""
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
                    record['is_annual_avg'] = True
                else:
                    record['is_annual_avg'] = False
                records.append(record)
            except Exception as e:
                logger.error(f"Error standardizing data point for {series_id}: {str(e)}")
    
    return pd.DataFrame(records)

def standardize_series(series_data: Dict[str, List]) -> pd.DataFrame:
    """Standardize raw series data into a clean DataFrame using chunking for memory efficiency."""
    if not series_data:
        logger.warning("No series data to standardize")
        return pd.DataFrame()
        
    extraction_timestamp = pd.Timestamp.now(tz='UTC')  # Single timestamp for all records
    start_time = time.time()
    
    # Create a list of (series_id, data_points) tuples
    data_items = list(series_data.items())
    total_items = len(data_items)
    
    # Process in chunks to reduce memory usage
    dfs = []
    for i in range(0, total_items, CHUNK_SIZE):
        chunk = data_items[i:i+CHUNK_SIZE]
        chunk_df = process_data_chunk(chunk, extraction_timestamp)
        dfs.append(chunk_df)
        
        if (i + CHUNK_SIZE) % (CHUNK_SIZE * 5) == 0 or (i + CHUNK_SIZE) >= total_items:
            logger.info(f"Processed {min(i + CHUNK_SIZE, total_items)}/{total_items} items")
            log_memory_usage()
    
    # Combine all DataFrames
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Standardization completed in {time.time() - start_time:.2f} seconds")
        return df
    else:
        logger.warning("No records were created during standardization")
        return pd.DataFrame()

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate the standardized data."""
    if df.empty:
        logger.warning("Empty DataFrame received, returning empty DataFrame")
        return df
    
    start_time = time.time()
    log_memory_usage()
    
    # Remove any duplicate records
    df = df.drop_duplicates()
    logger.info(f"After removing duplicates: {len(df)} records")
    
    # Convert period to standardized month format (M01 -> 1)
    # Fixed regex escape sequence
    df['month'] = df['period'].str.extract(r'M(\d+)').astype(int)
    
    # Create basic date column
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + 
                              df['month'].astype(str) + '-01')
    
    # Extract quarter information for partitioning
    df['quarter'] = df['date'].dt.quarter
    
    # Drop intermediate columns
    df = df.drop(['period', 'month'], axis=1)
    
    # Sort by series_id and date
    df = df.sort_values(['series_id', 'date'])
    
    logger.info(f"Data cleaning completed in {time.time() - start_time:.2f} seconds")
    log_memory_usage()
    
    return df

def save_to_parquet(df: pd.DataFrame, silver_dir: Path):
    """Save cleaned data to a single Parquet file with timestamp in the filename."""
    if df.empty:
        logger.warning("No data to save to Parquet")
        return
    
    start_time = time.time()
    
    # Convert to PyArrow Table with proper timestamp types
    table = pa.Table.from_pandas(
        df,
        schema=pa.schema([
            ('series_id', pa.string()),
            ('year', pa.int32()),
            ('quarter', pa.int32()),
            ('value', pa.float64()),
            ('footnotes', pa.string()),
            ('is_annual_avg', pa.bool_()),
            ('date', pa.timestamp('ns')),
            ('extraction_timestamp', pa.timestamp('ns', tz='UTC'))
        ])
    )
    
    # Create output directory if it doesn't exist
    output_dir = silver_dir / 'bls_data'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    output_file = output_dir / f'bls_data_{timestamp}.parquet'
    
    # Write single parquet file
    pq.write_table(
        table,
        output_file,
        compression='snappy'
    )
    
    logger.info(f"Data saved to {output_file} in {time.time() - start_time:.2f} seconds")
    log_memory_usage()

def main():
    """Main execution function."""
    start_time = time.time()
    
    # Define data directories
    bronze_dir = Path("data/bronze")
    silver_dir = Path("data/silver")
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting BLS data cleaning process with {MAX_WORKERS} workers")
    log_memory_usage()
    
    # Process validated PPI series file first
    process_validated_series(bronze_dir, silver_dir)
    
    logger.info("Loading raw series data from bronze layer...")
    raw_data = load_raw_series_data(bronze_dir)
    
    if not raw_data:
        logger.error("No raw data found. Exiting.")
        return
        
    logger.info(f"Loaded {len(raw_data)} series")
    
    logger.info("Standardizing series data...")
    df = standardize_series(raw_data)
    
    if df.empty:
        logger.error("No data after standardization. Exiting.")
        return
        
    logger.info(f"Created DataFrame with {len(df)} records")
    
    logger.info("Cleaning and validating data...")
    clean_df = clean_data(df)
    
    if clean_df.empty:
        logger.error("No data after cleaning. Exiting.")
        return
        
    logger.info(f"Final DataFrame shape: {clean_df.shape}")
    
    logger.info("Saving cleaned data to Parquet format...")
    save_to_parquet(clean_df, silver_dir)
    logger.info("Data successfully saved to silver layer")
    
    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f"Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    log_memory_usage()

if __name__ == "__main__":
    main() 