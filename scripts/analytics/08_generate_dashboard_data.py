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
import uuid

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

class UUIDEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle UUID objects."""
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

def get_series_id_by_raw_id(client, raw_series_id: str) -> Optional[str]:
    """Get the series_id for a given raw series ID."""
    query = f"""
    SELECT series_id
    FROM macro.gold_economic_indicators_metadata
    WHERE raw_series_id = '{raw_series_id}'
    LIMIT 1
    """
    result = client.query(query)
    return result.first_row[0] if result.row_count > 0 else None

def generate_key_indicators_dataset(client) -> Dict[str, Any]:
    """Generate a dataset of key economic indicators for a dashboard summary."""
    try:
        start_time = time.time()
        logger.info("Generating key indicators dataset")
        
        # Query to get all available indicators with their latest values
        query = """
        WITH latest_values AS (
            SELECT 
                m.series_id as series_id,
                m.indicator_name as indicator_name,
                m.category as category,
                m.subcategory as subcategory,
                v.month_date as date,
                v.avg_value as value,
                c.mom_change as mom_change,
                c.yoy_change as yoy_change
            FROM macro.gold_economic_indicators_monthly v
            JOIN macro.gold_economic_indicators_metadata m ON v.series_id = m.series_id
            LEFT JOIN macro.gold_economic_indicators_changes c 
                ON v.series_id = c.series_id 
                AND v.month_date = c.date
            WHERE (v.series_id, v.month_date) IN (
                SELECT series_id, max(month_date)
                FROM macro.gold_economic_indicators_monthly
                GROUP BY series_id
            )
        )
        SELECT 
            series_id,
            indicator_name,
            category,
            subcategory,
            date,
            value,
            mom_change,
            yoy_change
        FROM latest_values
        ORDER BY category, subcategory, indicator_name
        """
        
        result = client.query(query)
        
        # Transform to dictionary with category grouping
        indicators_data = {}
        
        for row in result.named_results():
            series_id = str(row['series_id'])
            category = row['category'] or 'Uncategorized'
            subcategory = row['subcategory'] or 'General'
            
            if category not in indicators_data:
                indicators_data[category] = {}
            
            if subcategory not in indicators_data[category]:
                indicators_data[category][subcategory] = {}
            
            indicators_data[category][subcategory][series_id] = {
                'series_id': series_id,
                'indicator_name': row['indicator_name'] or f"Indicator {series_id}",
                'date': row['date'].isoformat() if row['date'] else None,
                'value': float(row['value']) if row['value'] is not None else None,
                'mom_change': float(row['mom_change']) if row['mom_change'] is not None else None,
                'yoy_change': float(row['yoy_change']) if row['yoy_change'] is not None else None
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

def generate_historical_trends_dataset(client, category: str = None, months: int = 24) -> Dict[str, Any]:
    """Generate historical trend data for a category over the last N months."""
    try:
        start_time = time.time()
        logger.info(f"Generating historical trends dataset for category {category} over {months} months")
        
        # Query to get historical data
        category_filter = f"AND m.category = '{category}'" if category else ""
        query = f"""
        WITH monthly_data AS (
            SELECT 
                m.series_id as series_id,
                m.indicator_name as indicator_name,
                m.category as category,
                m.subcategory as subcategory,
                v.month_date as date,
                v.avg_value as value,
                c.mom_change as mom_change,
                c.yoy_change as yoy_change
            FROM macro.gold_economic_indicators_monthly v
            JOIN macro.gold_economic_indicators_metadata m ON v.series_id = m.series_id
            LEFT JOIN macro.gold_economic_indicators_changes c 
                ON v.series_id = c.series_id 
                AND v.month_date = c.date
            WHERE v.month_date >= dateAdd(month, -{months}, today())
            {category_filter}
        )
        SELECT 
            series_id,
            indicator_name,
            category,
            subcategory,
            date,
            value,
            mom_change,
            yoy_change
        FROM monthly_data
        ORDER BY category, subcategory, indicator_name, date
        """
        
        result = client.query(query)
        
        # Transform to dictionary grouped by category and series
        trends_data = {}
        
        for row in result.named_results():
            series_id = str(row['series_id'])
            category = row['category'] or 'Uncategorized'
            subcategory = row['subcategory'] or 'General'
            
            if category not in trends_data:
                trends_data[category] = {}
            
            if subcategory not in trends_data[category]:
                trends_data[category][subcategory] = {}
            
            if series_id not in trends_data[category][subcategory]:
                trends_data[category][subcategory][series_id] = {
                    'series_id': series_id,
                    'indicator_name': row['indicator_name'] or f"Indicator {series_id}",
                    'data_points': []
                }
            
            trends_data[category][subcategory][series_id]['data_points'].append({
                'date': row['date'].isoformat() if row['date'] else None,
                'value': float(row['value']) if row['value'] is not None else None,
                'mom_change': float(row['mom_change']) if row['mom_change'] is not None else None,
                'yoy_change': float(row['yoy_change']) if row['yoy_change'] is not None else None
            })
        
        # Add metadata
        dataset = {
            'timestamp': datetime.datetime.now().isoformat(),
            'months_included': months,
            'category_filter': category,
            'data': trends_data,
            'generation_time_seconds': round(time.time() - start_time, 2)
        }
        
        logger.info(f"Historical trends dataset generated in {dataset['generation_time_seconds']} seconds")
        return dataset
    
    except Exception as e:
        logger.error(f"Error generating historical trends dataset: {str(e)}")
        return {
            'timestamp': datetime.datetime.now().isoformat(),
            'error': str(e)
        }

def generate_correlation_matrix(client, category: str = None, months: int = 24) -> Dict[str, Any]:
    """Generate a correlation matrix for indicators in a category."""
    try:
        start_time = time.time()
        logger.info(f"Generating correlation matrix for category {category} over {months} months")
        
        # Query to get correlations
        category_filter = f"AND m1.category = '{category}' AND m2.category = '{category}'" if category else ""
        query = f"""
        WITH latest_correlations AS (
            SELECT 
                indicator_id_1,
                indicator_id_2,
                max(period_end) as max_period_end
            FROM macro.gold_economic_indicators_monthly_correlations
            WHERE period_end >= dateAdd(month, -{months}, today())
            GROUP BY indicator_id_1, indicator_id_2
        )
        
        SELECT 
            c.indicator_id_1,
            c.indicator_id_2,
            m1.indicator_name as name_1,
            m2.indicator_name as name_2,
            c.correlation,
            c.period_start,
            c.period_end
        FROM macro.gold_economic_indicators_monthly_correlations c
        JOIN latest_correlations lc 
            ON c.indicator_id_1 = lc.indicator_id_1 
            AND c.indicator_id_2 = lc.indicator_id_2
            AND c.period_end = lc.max_period_end
        JOIN macro.gold_economic_indicators_metadata m1 ON c.indicator_id_1 = m1.indicator_id
        JOIN macro.gold_economic_indicators_metadata m2 ON c.indicator_id_2 = m2.indicator_id
        WHERE c.period_end >= dateAdd(month, -{months}, today())
        {category_filter}
        ORDER BY m1.indicator_name, m2.indicator_name
        """
        
        result = client.query(query)
        
        # Transform to matrix format
        correlation_data = {}
        series_names = {}
        
        for row in result.named_results():
            id1 = str(row['indicator_id_1'])
            id2 = str(row['indicator_id_2'])
            
            # Store series names
            series_names[id1] = row['name_1'] or f"Indicator {id1}"
            series_names[id2] = row['name_2'] or f"Indicator {id2}"
            
            # Store correlation
            if id1 not in correlation_data:
                correlation_data[id1] = {}
            
            correlation_data[id1][id2] = float(row['correlation']) if row['correlation'] is not None else None
        
        # Add metadata
        dataset = {
            'timestamp': datetime.datetime.now().isoformat(),
            'months_included': months,
            'category_filter': category,
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
    """Save a dataset to a JSON file with timestamp."""
    try:
        # Create the output directory if it doesn't exist
        os.makedirs("data/dashboards", exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/dashboards/{name}_{timestamp}.json"
        
        # Save the dataset using the custom encoder
        with open(filename, 'w') as f:
            json.dump(dataset, f, cls=UUIDEncoder, indent=2)
        
        logger.info(f"Saved dataset to {filename}")
        return filename
    
    except Exception as e:
        logger.error(f"Error saving dataset: {str(e)}")
        return ""

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
        
        # Generate historical trends datasets for each category
        categories = ['industry', 'commodity', 'labor']
        for category in categories:
            logger.info(f"Generating historical trends dataset for {category}")
            historical_trends = generate_historical_trends_dataset(client, category=category, months=36)
            results["datasets_generated"].append(f"historical_trends_{category}")
            
            # Save dataset
            trends_path = save_dataset(historical_trends, f"historical_trends_{category}")
            if trends_path:
                results["datasets_saved"].append(trends_path)
        
        # Generate correlation matrices for each category
        for category in categories:
            logger.info(f"Generating correlation matrix for {category}")
            correlation_matrix = generate_correlation_matrix(client, category=category, months=36)
            results["datasets_generated"].append(f"correlation_matrix_{category}")
            
            # Save dataset
            matrix_path = save_dataset(correlation_matrix, f"correlation_matrix_{category}")
            if matrix_path:
                results["datasets_saved"].append(matrix_path)
        
        # Generate combined dashboard dataset
        logger.info("Generating combined dashboard dataset")
        dashboard_data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "key_indicators": key_indicators.get("data", {}),
            "historical_trends": {
                category: generate_historical_trends_dataset(client, category=category, months=36).get("data", {})
                for category in categories
            },
            "correlation_matrices": {
                category: generate_correlation_matrix(client, category=category, months=36).get("correlation_matrix", {})
                for category in categories
            }
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
        
    except Exception as e:
        logger.error(f"Error in dashboard data generation: {str(e)}")
        results["error"] = str(e)
    
    finally:
        logger.info("Dashboard data generation process completed")

if __name__ == "__main__":
    main() 