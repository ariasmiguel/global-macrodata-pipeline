import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_and_standardize_df(file_path: Path) -> pd.DataFrame:
    """Load CSV and standardize column names."""
    df = pd.read_csv(file_path)
    
    # Get original column names
    orig_cols = df.columns.tolist()
    logger.info(f"Original columns in {file_path.name}: {orig_cols}")
    
    # Standardize to series_id and series_name
    if len(df.columns) >= 2:
        df.columns = ['series_id', 'series_name'] + orig_cols[2:]
    
    return df[['series_id', 'series_name']]

def combine_series_data(dfs: dict) -> pd.DataFrame:
    """Combine series data handling duplicates and adding series type."""
    # Start with aggregation data
    final_df = dfs['aggregation'].copy()
    final_df['series_type'] = 'aggregated'
    
    # Get series IDs to exclude from other dataframes
    existing_series = set(final_df['series_id'])
    
    # Add commodity data (excluding duplicates)
    commodity_mask = ~dfs['commodity']['series_id'].isin(existing_series)
    commodity_unique = dfs['commodity'][commodity_mask].copy()
    commodity_unique.loc[:, 'series_type'] = 'commodity'
    final_df = pd.concat([final_df, commodity_unique], ignore_index=True)
    
    # Add industry data (excluding duplicates)
    existing_series = set(final_df['series_id'])
    industry_mask = ~dfs['industry']['series_id'].isin(existing_series)
    industry_unique = dfs['industry'][industry_mask].copy()
    industry_unique.loc[:, 'series_type'] = 'industry'
    final_df = pd.concat([final_df, industry_unique], ignore_index=True)
    
    return final_df

def main():
    """Main function to combine PPI series data."""
    try:
        bronze_dir = Path("data/bronze")
        
        # Load all dataframes
        files = {
            'aggregation': 'ppi_aggregation_series_ids.csv',
            'commodity': 'ppi_commodity_series_ids.csv',
            'industry': 'ppi_series_ids.csv'
        }
        
        dfs = {}
        for df_type, filename in files.items():
            file_path = bronze_dir / filename
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                continue
                
            dfs[df_type] = load_and_standardize_df(file_path)
            logger.info(f"Loaded {df_type} data: {len(dfs[df_type])} rows")
        
        # Analyze duplicates before combining
        all_series = pd.concat(dfs.values(), ignore_index=True)
        total_rows = len(all_series)
        unique_rows = len(all_series.drop_duplicates())
        duplicate_rows = total_rows - unique_rows
        
        logger.info("\nInitial Duplicate Analysis:")
        logger.info(f"Total rows: {total_rows}")
        logger.info(f"Unique rows: {unique_rows}")
        logger.info(f"Duplicate rows: {duplicate_rows}")
        
        # Combine data handling duplicates
        final_df = combine_series_data(dfs)
        
        # Save combined data
        output_path = bronze_dir / 'final_ppi_series_ids.csv'
        final_df.to_csv(output_path, index=False)
        
        # Log summary statistics
        logger.info("\nFinal Dataset Summary:")
        logger.info(f"Total unique series: {len(final_df)}")
        logger.info("\nSeries by type:")
        type_counts = final_df['series_type'].value_counts()
        for series_type, count in type_counts.items():
            logger.info(f"{series_type}: {count} series")
        
        logger.info(f"\nSaved combined dataset to {output_path}")
    
    except Exception as e:
        logger.error(f"Error processing PPI series data: {str(e)}")
        raise

if __name__ == "__main__":
    main() 