"""
Script to generate dashboard-ready datasets from the gold layer.
This is step 8 in the Macroeconomic Data Pipeline - preparing data for visualization dashboards.
"""

import os
import logging
import json
import datetime
import time
from pathlib import Path
import clickhouse_connect
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np

# Configure logging
os.makedirs("logs/analytics", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/analytics/08_generate_dashboard_data.log')
    ]
)
logger = logging.getLogger(__name__)

def connect_to_clickhouse():
    """Connect to ClickHouse database."""
    try:
        # Get connection details from environment variables or use defaults
        host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
        port = int(os.environ.get('CLICKHOUSE_PORT', 8123))
        username = os.environ.get('CLICKHOUSE_USER', 'default')
        password = os.environ.get('CLICKHOUSE_PASSWORD', '')
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

def generate_key_indicators_dataset(client) -> Dict[str, Any]:
    """Generate a dataset of key economic indicators for a dashboard summary."""
    try:
        start_time = time.time()
        logger.info("Generating key indicators dataset")
        
        # Define key indicator series IDs
        key_indicators = {
            "ppi_all_commodities": "WPU00000000",         # PPI - All Commodities
            "ppi_final_demand": "WPUFD4",                # PPI - Final Demand
            "ppi_intermediate_demand": "WPUID61",         # PPI - Intermediate Demand
            "ppi_goods": "WPUFD41",                      # PPI - Goods
            "ppi_services": "WPUFD42",                   # PPI - Services
            "unemployment_rate": "LNS14000000",          # Unemployment Rate
            "labor_participation": "LNS11300000"         # Labor Force Participation Rate
        }
        
        # Query to get the latest values for each indicator
        query = f"""
        WITH latest_dates AS (
            SELECT series_id, max(date) as max_date
            FROM macro.gold_economic_indicators_values
            WHERE series_id IN ({', '.join([f"'{id}'" for id in key_indicators.values()])})
            GROUP BY series_id
        )
        
        SELECT 
            v.series_id,
            m.indicator_name,
            v.date,
            v.value,
            c.mom_change,
            c.yoy_change
        FROM macro.gold_economic_indicators_values v
        JOIN latest_dates ld ON v.series_id = ld.series_id AND v.date = ld.max_date
        JOIN macro.gold_economic_indicators_metadata m ON v.series_id = m.series_id
        LEFT JOIN macro.gold_economic_indicators_changes c ON v.series_id = c.series_id AND v.date = c.date
        """
        
        result = client.query(query)
        
        # Transform to dictionary with indicator name as key
        indicators_data = {}
        
        for row in result.named_results():
            series_id = row['series_id']
            
            # Find the friendly name based on the series_id
            friendly_name = None
            for name, id in key_indicators.items():
                if id == series_id:
                    friendly_name = name
                    break
            
            if friendly_name:
                indicators_data[friendly_name] = {
                    'series_id': series_id,
                    'indicator_name': row['indicator_name'],
                    'date': row['date'].isoformat() if row['date'] else None,
                    'value': row['value'],
                    'mom_change': row['mom_change'],
                    'yoy_change': row['yoy_change']
                }
        
        # Add metadata
        dataset = {
            'timestamp': datetime.datetime.now().isoformat(),
            'data': indicators_data,
            'generation_time_seconds': round(time.time() - start_time, 2)
        }
        
        logger.info(f"Key indicators dataset generated in {dataset['generation_time_seconds']} seconds")
        return dataset
    
    except Exception as e:
        logger.error(f"Error generating key indicators dataset: {str(e)}")
        return {
            'timestamp': datetime.datetime.now().isoformat(),
            'error': str(e)
        }

def generate_historical_trends_dataset(client, series_ids: List[str], months: int = 24) -> Dict[str, Any]:
    """Generate historical trend data for specified series over the last N months."""
    try:
        start_time = time.time()
        logger.info(f"Generating historical trends dataset for {len(series_ids)} series over {months} months")
        
        # Convert series IDs to a string for the query
        series_ids_str = ', '.join([f"'{id}'" for id in series_ids])
        
        # Query to get historical data for the specified series
        query = f"""
        WITH series_dates AS (
            SELECT 
                series_id,
                date
            FROM macro.gold_economic_indicators_values
            WHERE series_id IN ({series_ids_str})
              AND date >= dateAdd(month, -{months}, today())
            ORDER BY series_id, date
        )
        
        SELECT 
            v.series_id,
            m.indicator_name,
            v.date,
            v.value,
            c.mom_change,
            c.yoy_change
        FROM macro.gold_economic_indicators_values v
        JOIN series_dates sd ON v.series_id = sd.series_id AND v.date = sd.date
        JOIN macro.gold_economic_indicators_metadata m ON v.series_id = m.series_id
        LEFT JOIN macro.gold_economic_indicators_changes c ON v.series_id = c.series_id AND v.date = c.date
        ORDER BY v.series_id, v.date
        """
        
        result = client.query(query)
        
        # Transform to dictionary grouped by series
        series_data = {}
        
        for row in result.named_results():
            series_id = row['series_id']
            
            if series_id not in series_data:
                series_data[series_id] = {
                    'indicator_name': row['indicator_name'],
                    'data_points': []
                }
            
            series_data[series_id]['data_points'].append({
                'date': row['date'].isoformat() if row['date'] else None,
                'value': row['value'],
                'mom_change': row['mom_change'],
                'yoy_change': row['yoy_change']
            })
        
        # Add metadata
        dataset = {
            'timestamp': datetime.datetime.now().isoformat(),
            'months_included': months,
            'series_count': len(series_data),
            'data': series_data,
            'generation_time_seconds': round(time.time() - start_time, 2)
        }
        
        logger.info(f"Historical trends dataset generated in {dataset['generation_time_seconds']} seconds with {len(series_data)} series")
        return dataset
    
    except Exception as e:
        logger.error(f"Error generating historical trends dataset: {str(e)}")
        return {
            'timestamp': datetime.datetime.now().isoformat(),
            'error': str(e)
        }

def generate_category_comparison_dataset(client, category: str) -> Dict[str, Any]:
    """Generate dataset comparing different indicators within the same category."""
    try:
        start_time = time.time()
        logger.info(f"Generating category comparison dataset for {category}")
        
        # Query to get metadata for the category
        metadata_query = f"""
        SELECT 
            series_id,
            indicator_name
        FROM macro.gold_economic_indicators_metadata
        WHERE category = '{category}'
        """
        
        metadata_result = client.query(metadata_query)
        
        # Get the series IDs in this category
        series_info = []
        series_ids = []
        
        for row in metadata_result.named_results():
            series_info.append({
                'series_id': row['series_id'],
                'indicator_name': row['indicator_name']
            })
            series_ids.append(row['series_id'])
        
        if not series_ids:
            logger.warning(f"No series found for category: {category}")
            return {
                'timestamp': datetime.datetime.now().isoformat(),
                'category': category,
                'error': f"No series found for category: {category}"
            }
        
        # Convert series IDs to a string for the query
        series_ids_str = ', '.join([f"'{id}'" for id in series_ids])
        
        # Query to get latest values for each series in the category
        values_query = f"""
        WITH latest_dates AS (
            SELECT 
                series_id,
                max(date) as max_date
            FROM macro.gold_economic_indicators_values
            WHERE series_id IN ({series_ids_str})
            GROUP BY series_id
        )
        
        SELECT 
            v.series_id,
            v.date,
            v.value,
            c.yoy_change
        FROM macro.gold_economic_indicators_values v
        JOIN latest_dates ld ON v.series_id = ld.series_id AND v.date = ld.max_date
        LEFT JOIN macro.gold_economic_indicators_changes c ON v.series_id = c.series_id AND v.date = c.date
        """
        
        values_result = client.query(values_query)
        
        # Combine metadata with values
        comparison_data = {}
        
        for row in values_result.named_results():
            series_id = row['series_id']
            
            # Find the indicator name
            indicator_name = next((item['indicator_name'] for item in series_info if item['series_id'] == series_id), series_id)
            
            comparison_data[series_id] = {
                'indicator_name': indicator_name,
                'date': row['date'].isoformat() if row['date'] else None,
                'value': row['value'],
                'yoy_change': row['yoy_change']
            }
        
        # Add metadata
        dataset = {
            'timestamp': datetime.datetime.now().isoformat(),
            'category': category,
            'series_count': len(comparison_data),
            'data': comparison_data,
            'generation_time_seconds': round(time.time() - start_time, 2)
        }
        
        logger.info(f"Category comparison dataset generated in {dataset['generation_time_seconds']} seconds with {len(comparison_data)} series")
        return dataset
    
    except Exception as e:
        logger.error(f"Error generating category comparison dataset: {str(e)}")
        return {
            'timestamp': datetime.datetime.now().isoformat(),
            'category': category,
            'error': str(e)
        }

def generate_correlation_matrix(client, series_ids: List[str], months: int = 24) -> Dict[str, Any]:
    """Generate a correlation matrix between specified indicators over time."""
    try:
        start_time = time.time()
        logger.info(f"Generating correlation matrix for {len(series_ids)} series over {months} months")
        
        # Convert series IDs to a string for the query
        series_ids_str = ', '.join([f"'{id}'" for id in series_ids])
        
        # Query to get historical data for all specified series
        query = f"""
        SELECT 
            v.series_id,
            m.indicator_name,
            v.date,
            v.value
        FROM macro.gold_economic_indicators_values v
        JOIN macro.gold_economic_indicators_metadata m ON v.series_id = m.series_id
        WHERE v.series_id IN ({series_ids_str})
          AND v.date >= dateAdd(month, -{months}, today())
        ORDER BY v.date, v.series_id
        """
        
        result = client.query(query)
        
        # Load data into pandas DataFrame
        records = []
        for row in result.named_results():
            records.append({
                'series_id': row['series_id'],
                'indicator_name': row['indicator_name'],
                'date': row['date'],
                'value': row['value']
            })
        
        df = pd.DataFrame(records)
        
        if df.empty:
            logger.warning(f"No data found for the specified series IDs")
            return {
                'timestamp': datetime.datetime.now().isoformat(),
                'error': "No data found for the specified series IDs"
            }
        
        # Create a pivot table with dates as index and series as columns
        pivot_df = df.pivot(index='date', columns='series_id', values='value')
        
        # Calculate correlation matrix
        corr_matrix = pivot_df.corr().fillna(0).round(3)
        
        # Convert to dictionary format
        correlation_data = {}
        series_names = {}
        
        # Get indicator names for each series
        for series_id in series_ids:
            indicator_name = df[df['series_id'] == series_id]['indicator_name'].iloc[0] if not df[df['series_id'] == series_id].empty else series_id
            series_names[series_id] = indicator_name
            correlation_data[series_id] = {}
        
        # Fill in correlation values
        for s1 in series_ids:
            for s2 in series_ids:
                if s1 in corr_matrix.index and s2 in corr_matrix.columns:
                    correlation_data[s1][s2] = float(corr_matrix.loc[s1, s2])
                else:
                    correlation_data[s1][s2] = None
        
        # Add metadata
        dataset = {
            'timestamp': datetime.datetime.now().isoformat(),
            'months_included': months,
            'series_names': series_names,
            'correlation_matrix': correlation_data,
            'generation_time_seconds': round(time.time() - start_time, 2)
        }
        
        logger.info(f"Correlation matrix generated in {dataset['generation_time_seconds']} seconds")
        return dataset
    
    except Exception as e:
        logger.error(f"Error generating correlation matrix: {str(e)}")
        return {
            'timestamp': datetime.datetime.now().isoformat(),
            'error': str(e)
        }

def save_dataset(dataset: Dict[str, Any], name: str) -> str:
    """Save a dataset to a JSON file in the dashboard data directory."""
    try:
        # Create directory if it doesn't exist
        data_dir = Path("data/dashboards")
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.json"
        file_path = data_dir / filename
        
        # Save to file
        with open(file_path, 'w') as f:
            json.dump(dataset, f, indent=2)
        
        logger.info(f"Saved dataset to {file_path}")
        return str(file_path)
    
    except Exception as e:
        logger.error(f"Error saving dataset: {str(e)}")
        return None

def main():
    """Main execution function."""
    start_time = time.time()
    
    logger.info("Starting dashboard data generation")
    
    results = {
        "timestamp": datetime.datetime.now().isoformat(),
        "datasets_generated": [],
        "datasets_saved": []
    }
    
    try:
        # Connect to ClickHouse
        client = connect_to_clickhouse()
        
        # Generate key indicators dataset
        logger.info("Generating key indicators dataset")
        key_indicators = generate_key_indicators_dataset(client)
        results["datasets_generated"].append("key_indicators")
        
        # Save dataset
        key_indicators_path = save_dataset(key_indicators, "key_indicators")
        if key_indicators_path:
            results["datasets_saved"].append(key_indicators_path)
        
        # Define series for historical trends
        ppi_series = [
            "WPU00000000",  # All Commodities
            "WPUFD4",       # Final Demand
            "WPUFD41",      # Goods
            "WPUFD42"       # Services
        ]
        
        # Generate historical trends dataset
        logger.info("Generating historical trends dataset")
        historical_trends = generate_historical_trends_dataset(client, ppi_series, months=36)
        results["datasets_generated"].append("historical_trends")
        
        # Save dataset
        historical_trends_path = save_dataset(historical_trends, "historical_trends")
        if historical_trends_path:
            results["datasets_saved"].append(historical_trends_path)
        
        # Generate category comparison dataset for PPI
        logger.info("Generating PPI category comparison dataset")
        ppi_comparison = generate_category_comparison_dataset(client, "PPI")
        results["datasets_generated"].append("ppi_comparison")
        
        # Save dataset
        ppi_comparison_path = save_dataset(ppi_comparison, "ppi_comparison")
        if ppi_comparison_path:
            results["datasets_saved"].append(ppi_comparison_path)
        
        # Define series for correlation matrix
        correlation_series = [
            "WPU00000000",  # All Commodities
            "WPUFD4",       # Final Demand
            "WPUFD41",      # Goods
            "WPUFD42",      # Services
            "LNS14000000"   # Unemployment Rate
        ]
        
        # Generate correlation matrix
        logger.info("Generating correlation matrix")
        correlation_matrix = generate_correlation_matrix(client, correlation_series, months=36)
        results["datasets_generated"].append("correlation_matrix")
        
        # Save dataset
        correlation_path = save_dataset(correlation_matrix, "correlation_matrix")
        if correlation_path:
            results["datasets_saved"].append(correlation_path)
        
        # Generate combined dashboard dataset with all data
        logger.info("Generating combined dashboard dataset")
        dashboard_data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "key_indicators": key_indicators.get("data", {}),
            "historical_trends": historical_trends.get("data", {}),
            "ppi_comparison": ppi_comparison.get("data", {}),
            "correlation_matrix": correlation_matrix.get("correlation_matrix", {})
        }
        
        # Save combined dataset
        dashboard_path = save_dataset(dashboard_data, "dashboard_combined")
        if dashboard_path:
            results["datasets_generated"].append("dashboard_combined")
            results["datasets_saved"].append(dashboard_path)
        
        # Add summary statistics
        end_time = time.time()
        total_time = end_time - start_time
        
        results["datasets_count"] = len(results["datasets_generated"])
        results["execution_time_seconds"] = round(total_time, 2)
        
        logger.info("====== Dashboard Data Generation Summary ======")
        logger.info(f"Datasets generated: {results['datasets_count']}")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        
        # Save summary to logs
        summary_dir = Path("data/logs")
        summary_dir.mkdir(parents=True, exist_ok=True)
        
        with open(summary_dir / f"dashboard_generation_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
            json.dump(results, f, indent=2)
        
    except Exception as e:
        logger.error(f"Error in dashboard data generation: {str(e)}")
        results["error"] = str(e)
        
        # Save error to logs
        summary_dir = Path("data/logs")
        summary_dir.mkdir(parents=True, exist_ok=True)
        
        with open(summary_dir / f"dashboard_generation_error_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
            json.dump(results, f, indent=2)
    
    finally:
        logger.info("Dashboard data generation process completed")

if __name__ == "__main__":
    main() 