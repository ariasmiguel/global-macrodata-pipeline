"""
Script to load gold layer data into ClickHouse gold tables.
This is step 7 in the Macroeconomic Data Pipeline - loading analytics-ready data into ClickHouse gold layer tables.
"""

import os
import logging
from pathlib import Path
import pandas as pd
import clickhouse_connect
import time
from typing import Dict, Any, List
import datetime
import json

# Configure logging
os.makedirs("logs/analytics", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/analytics/07_load_gold_to_clickhouse.log')
    ]
)
logger = logging.getLogger(__name__)

def connect_to_clickhouse():
    """Connect to ClickHouse database."""
    try:
        # Get connection details from environment variables or use defaults
        host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
        port = 8123  # HTTP port hardcoded for simplicity
        username = os.environ.get('CLICKHOUSE_USER', 'default')
        password = os.environ.get('CLICKHOUSE_PASSWORD', 'clickhouse')
        database = os.environ.get('CLICKHOUSE_DB', 'macro')
        
        logger.info(f"Connecting to ClickHouse at {host}:{port}, database: {database}")
        
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database
        )
        
        # Test connection
        version = client.query("SELECT version()").first_row[0]
        logger.info(f"Successfully connected to ClickHouse version {version}")
        return client
    
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {str(e)}")
        raise

def check_gold_tables(client):
    """Check if gold tables exist and have data."""
    try:
        # List of tables to check
        tables = [
            "macro.gold_economic_indicators_metadata",
            "macro.gold_economic_indicators_values",
            "macro.gold_economic_indicators_monthly",
            "macro.gold_economic_indicators_yearly",
            "macro.gold_economic_indicators_changes"
        ]
        
        results = {}
        
        for table in tables:
            try:
                # Check if table exists
                exists_query = f"""
                SELECT 1 
                FROM system.tables 
                WHERE database = 'macro' 
                  AND name = '{table.split('.')[1]}'
                """
                exists = client.query(exists_query).row_count > 0
                
                if exists:
                    # Count rows
                    count_query = f"SELECT count(*) FROM {table}"
                    count = client.query(count_query).first_row[0]
                    
                    results[table] = {
                        "exists": True,
                        "row_count": count
                    }
                    
                    logger.info(f"Table {table} exists with {count} rows")
                else:
                    results[table] = {
                        "exists": False,
                        "row_count": 0
                    }
                    logger.warning(f"Table {table} does not exist")
            
            except Exception as e:
                results[table] = {
                    "exists": False,
                    "error": str(e)
                }
                logger.error(f"Error checking table {table}: {str(e)}")
        
        return results
    
    except Exception as e:
        logger.error(f"Error checking gold tables: {str(e)}")
        return {"error": str(e)}

def generate_summary_stats(client):
    """Generate summary statistics for the gold layer data."""
    try:
        start_time = time.time()
        
        stats = {
            "timestamp": datetime.datetime.now().isoformat(),
            "metadata": {},
            "values": {},
            "monthly_view": {},
            "yearly_view": {},
            "changes_view": {},
            "correlations": {}
        }
        
        # Metadata stats
        logger.info("Generating metadata statistics")
        metadata_query = """
        SELECT
            count(*) AS total_indicators,
            countDistinct(category) AS categories_count,
            groupArray(category) AS categories
        FROM macro.gold_economic_indicators_metadata
        """
        metadata_results = client.query(metadata_query).first_row
        
        stats["metadata"]["total_indicators"] = metadata_results[0]
        stats["metadata"]["categories_count"] = metadata_results[1]
        stats["metadata"]["categories"] = metadata_results[2]
        
        # Values stats
        logger.info("Generating values statistics")
        values_query = """
        SELECT
            count(*) AS total_values,
            countDistinct(series_id) AS distinct_series,
            min(date) AS min_date,
            max(date) AS max_date,
            countIf(is_annual_avg = 1) AS annual_avg_count
        FROM macro.gold_economic_indicators_values
        """
        values_results = client.query(values_query).first_row
        
        stats["values"]["total_values"] = values_results[0]
        stats["values"]["distinct_series"] = values_results[1]
        stats["values"]["min_date"] = values_results[2].isoformat() if values_results[2] else None
        stats["values"]["max_date"] = values_results[3].isoformat() if values_results[3] else None
        stats["values"]["annual_avg_count"] = values_results[4]
        
        # Monthly view stats
        logger.info("Generating monthly view statistics")
        monthly_query = """
        SELECT
            count(*) AS total_values,
            countDistinct(series_id) AS distinct_series,
            min(month_date) AS min_date,
            max(month_date) AS max_date
        FROM macro.gold_economic_indicators_monthly
        """
        monthly_results = client.query(monthly_query).first_row
        
        stats["monthly_view"]["total_values"] = monthly_results[0]
        stats["monthly_view"]["distinct_series"] = monthly_results[1]
        stats["monthly_view"]["min_date"] = monthly_results[2].isoformat() if monthly_results[2] else None
        stats["monthly_view"]["max_date"] = monthly_results[3].isoformat() if monthly_results[3] else None
        
        # Yearly view stats
        logger.info("Generating yearly view statistics")
        yearly_query = """
        SELECT
            count(*) AS total_values,
            countDistinct(series_id) AS distinct_series,
            min(year) AS min_year,
            max(year) AS max_year
        FROM macro.gold_economic_indicators_yearly
        """
        yearly_results = client.query(yearly_query).first_row
        
        stats["yearly_view"]["total_values"] = yearly_results[0]
        stats["yearly_view"]["distinct_series"] = yearly_results[1]
        stats["yearly_view"]["min_year"] = yearly_results[2]
        stats["yearly_view"]["max_year"] = yearly_results[3]
        
        # Changes view stats
        logger.info("Generating changes view statistics")
        changes_query = """
        SELECT
            count(*) AS total_values,
            countDistinct(series_id) AS distinct_series,
            min(date) AS min_date,
            max(date) AS max_date
        FROM macro.gold_economic_indicators_changes
        """
        changes_results = client.query(changes_query).first_row
        
        stats["changes_view"]["total_values"] = changes_results[0]
        stats["changes_view"]["distinct_series"] = changes_results[1]
        stats["changes_view"]["min_date"] = changes_results[2].isoformat() if changes_results[2] else None
        stats["changes_view"]["max_date"] = changes_results[3].isoformat() if changes_results[3] else None
        
        # Correlations stats
        logger.info("Generating correlations statistics")
        correlations_query = """
        SELECT
            count(*) AS total_correlations,
            countDistinct(indicator_id_1) AS distinct_indicators_1,
            countDistinct(indicator_id_2) AS distinct_indicators_2,
            min(period_start) AS min_period_start,
            max(period_end) AS max_period_end,
            avg(correlation) AS avg_correlation,
            min(correlation) AS min_correlation,
            max(correlation) AS max_correlation
        FROM macro.gold_economic_indicators_correlations
        """
        correlations_results = client.query(correlations_query).first_row
        
        stats["correlations"]["total_correlations"] = correlations_results[0]
        stats["correlations"]["distinct_indicators_1"] = correlations_results[1]
        stats["correlations"]["distinct_indicators_2"] = correlations_results[2]
        stats["correlations"]["min_period_start"] = correlations_results[3].isoformat() if correlations_results[3] else None
        stats["correlations"]["max_period_end"] = correlations_results[4].isoformat() if correlations_results[4] else None
        stats["correlations"]["avg_correlation"] = round(correlations_results[5], 3) if correlations_results[5] else None
        stats["correlations"]["min_correlation"] = round(correlations_results[6], 3) if correlations_results[6] else None
        stats["correlations"]["max_correlation"] = round(correlations_results[7], 3) if correlations_results[7] else None
        
        # Add total runtime
        stats["execution_time_seconds"] = round(time.time() - start_time, 2)
        
        logger.info(f"Summary statistics generation completed in {stats['execution_time_seconds']} seconds")
        return stats
    
    except Exception as e:
        logger.error(f"Error generating summary statistics: {str(e)}")
        return {
            "timestamp": datetime.datetime.now().isoformat(),
            "error": str(e)
        }

def generate_sample_data(client):
    """Extract sample data for each table in the gold layer for validation."""
    try:
        start_time = time.time()
        
        samples = {
            "timestamp": datetime.datetime.now().isoformat(),
            "metadata_sample": [],
            "values_sample": [],
            "monthly_sample": [],
            "yearly_sample": [],
            "changes_sample": []
        }
        
        # Metadata sample
        logger.info("Extracting metadata sample")
        metadata_query = """
        SELECT * 
        FROM macro.gold_economic_indicators_metadata
        LIMIT 10
        """
        metadata_result = client.query(metadata_query)
        
        for row in metadata_result.named_results():
            # Convert to JSON-serializable format
            row_dict = {k: str(v) if isinstance(v, (datetime.date, datetime.datetime)) else v 
                        for k, v in row.items()}
            samples["metadata_sample"].append(row_dict)
        
        # Values sample
        logger.info("Extracting values sample")
        values_query = """
        SELECT * 
        FROM macro.gold_economic_indicators_values
        ORDER BY date DESC
        LIMIT 10
        """
        values_result = client.query(values_query)
        
        for row in values_result.named_results():
            # Convert to JSON-serializable format
            row_dict = {k: str(v) if isinstance(v, (datetime.date, datetime.datetime)) else v 
                        for k, v in row.items()}
            samples["values_sample"].append(row_dict)
        
        # Monthly sample
        logger.info("Extracting monthly sample")
        monthly_query = """
        SELECT * 
        FROM macro.gold_economic_indicators_monthly
        ORDER BY month_date DESC
        LIMIT 10
        """
        monthly_result = client.query(monthly_query)
        
        for row in monthly_result.named_results():
            # Convert to JSON-serializable format
            row_dict = {k: str(v) if isinstance(v, (datetime.date, datetime.datetime)) else v 
                        for k, v in row.items()}
            samples["monthly_sample"].append(row_dict)
        
        # Yearly sample
        logger.info("Extracting yearly sample")
        yearly_query = """
        SELECT * 
        FROM macro.gold_economic_indicators_yearly
        ORDER BY year DESC
        LIMIT 10
        """
        yearly_result = client.query(yearly_query)
        
        for row in yearly_result.named_results():
            # Convert to JSON-serializable format
            row_dict = {k: str(v) if isinstance(v, (datetime.date, datetime.datetime)) else v 
                        for k, v in row.items()}
            samples["yearly_sample"].append(row_dict)
        
        # Changes sample
        logger.info("Extracting changes sample")
        changes_query = """
        SELECT * 
        FROM macro.gold_economic_indicators_changes
        ORDER BY date DESC
        LIMIT 10
        """
        changes_result = client.query(changes_query)
        
        for row in changes_result.named_results():
            # Convert to JSON-serializable format
            row_dict = {k: str(v) if isinstance(v, (datetime.date, datetime.datetime)) else v 
                        for k, v in row.items()}
            samples["changes_sample"].append(row_dict)
        
        # Add total runtime
        samples["execution_time_seconds"] = round(time.time() - start_time, 2)
        
        logger.info(f"Sample data extraction completed in {samples['execution_time_seconds']} seconds")
        return samples
    
    except Exception as e:
        logger.error(f"Error generating sample data: {str(e)}")
        return {
            "timestamp": datetime.datetime.now().isoformat(),
            "error": str(e)
        }

def save_report(data, filename):
    """Save data to a JSON file in the reports directory."""
    try:
        reports_dir = Path("data/reports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = reports_dir / filename
        
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved report to {file_path}")
        return str(file_path)
    
    except Exception as e:
        logger.error(f"Error saving report: {str(e)}")
        return None

def load_gold_data(client) -> Dict[str, int]:
    """Load gold layer data into ClickHouse.
    
    Args:
        client: ClickHouse client
        
    Returns:
        Dict with counts of loaded records
    """
    try:
        logger.info("Loading gold layer data")
        logger.info("Loading gold layer data into ClickHouse")
        
        # Load metadata
        metadata_file = Path("data/gold/gold_metadata_*.parquet")
        metadata_files = list(metadata_file.parent.glob(metadata_file.name))
        if not metadata_files:
            raise FileNotFoundError("No gold metadata files found")
        
        metadata_file = metadata_files[0]  # Use the most recent file
        logger.info(f"Loading metadata from {metadata_file}")
        
        metadata_df = pd.read_parquet(metadata_file)
        client.insert_df("macro.gold_economic_indicators_metadata", metadata_df)
        metadata_count = len(metadata_df)
        logger.info(f"Loaded {metadata_count} metadata records")
        
        # Load values
        values_file = Path("data/gold/gold_values_*.parquet")
        values_files = list(values_file.parent.glob(values_file.name))
        if not values_files:
            raise FileNotFoundError("No gold values files found")
        
        values_file = values_files[0]  # Use the most recent file
        logger.info(f"Loading values from {values_file}")
        
        values_df = pd.read_parquet(values_file)
        client.insert_df("macro.gold_economic_indicators_values", values_df)
        values_count = len(values_df)
        logger.info(f"Loaded {values_count} values records")
        
        # Skip correlations loading
        logger.info("Skipping correlations loading")
        
        return {
            "metadata_count": metadata_count,
            "values_count": values_count,
            "correlations_count": 0
        }
        
    except Exception as e:
        logger.error(f"Error loading gold data: {str(e)}")
        raise

def main():
    """Main execution function."""
    try:
        # Connect to ClickHouse
        client = connect_to_clickhouse()
        
        # Check tables
        logger.info("Checking gold tables")
        table_status = check_gold_tables(client)
        
        # Load gold data
        logger.info("Loading gold layer data")
        load_results = load_gold_data(client)
        
        # Generate summary statistics
        logger.info("Generating summary statistics")
        stats = generate_summary_stats(client)
        
        # Generate sample data
        logger.info("Generating sample data")
        samples = generate_sample_data(client)
        
        # Save reports
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        save_report(stats, f"data/reports/gold_summary_stats_{timestamp}.json")
        save_report(samples, f"data/reports/gold_sample_data_{timestamp}.json")
        
        # Create validation report
        validation_report = {
            "timestamp": datetime.datetime.now().isoformat(),
            "table_status": table_status,
            "load_results": load_results,
            "summary_stats": stats,
            "execution_time_minutes": round((time.time() - start_time) / 60, 2)
        }
        save_report(validation_report, f"data/reports/gold_validation_report_{timestamp}.json")
        
        # Print summary
        logger.info("====== Gold Layer Validation Summary ======")
        logger.info(f"Tables checked: {len(table_status)}")
        logger.info(f"Total indicators: {stats['metadata']['total_indicators']}")
        logger.info(f"Total values: {stats['values']['total_values']}")
        logger.info(f"Reports saved: 3")
        logger.info(f"Total execution time: {validation_report['execution_time_minutes']:.2f} minutes")
        logger.info("===========================================")
        
        logger.info("Gold layer validation process completed")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    start_time = time.time()
    main() 