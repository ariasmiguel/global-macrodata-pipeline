"""
Script to validate data loaded into ClickHouse.
This script performs various checks to ensure data quality and completeness.
"""

import os
from clickhouse_connect import get_client
from loguru import logger
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Configure logger
logger.add(
    "logs/validation/validate_clickhouse_data.log",
    rotation="500 MB",
    retention="10 days",
    level="INFO"
)

def check_table_structure(client) -> None:
    """Check if all required tables exist and have the correct structure."""
    logger.info("Checking table structure...")
    
    # Expected tables and their minimum row counts
    expected_tables = {
        'macro.economic_indicators_metadata': 0,
        'macro.economic_indicators_values': 0,
        'macro.economic_indicators_monthly': 0,
        'macro.economic_indicators_changes': 0
    }
    
    for table in expected_tables:
        try:
            result = client.query(f"SELECT count() FROM {table}")
            count = result.result_rows[0][0]
            expected_tables[table] = count
            logger.info(f"Table {table} has {count:,} rows")
        except Exception as e:
            logger.error(f"Error checking table {table}: {str(e)}")
            raise
    
    return expected_tables

def check_data_completeness(client) -> None:
    """Check for data completeness and gaps."""
    logger.info("Checking data completeness...")
    
    # Check date ranges
    queries = [
        """
        SELECT 
            min(date) as min_date,
            max(date) as max_date,
            count(DISTINCT date) as unique_dates
        FROM macro.economic_indicators_values
        """,
        """
        SELECT 
            count() as total_values,
            countIf(value IS NULL) as null_values,
            round(countIf(value IS NULL) / count() * 100, 2) as null_percentage
        FROM macro.economic_indicators_values
        """,
        """
        SELECT 
            count(DISTINCT series_id) as unique_series,
            count(DISTINCT date) as unique_dates,
            count() as total_values
        FROM macro.economic_indicators_values
        """
    ]
    
    for query in queries:
        try:
            result = client.query(query)
            logger.info(f"Query result: {result.result_rows[0]}")
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

def check_materialized_views(client) -> None:
    """Check if materialized views are populated correctly."""
    logger.info("Checking materialized views...")
    
    queries = [
        """
        SELECT 
            count() as total_monthly_records,
            min(date) as min_date,
            max(date) as max_date
        FROM macro.economic_indicators_monthly
        """,
        """
        SELECT 
            count() as total_changes_records,
            min(date) as min_date,
            max(date) as max_date
        FROM macro.economic_indicators_changes
        """
    ]
    
    for query in queries:
        try:
            result = client.query(query)
            logger.info(f"Query result: {result.result_rows[0]}")
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

def check_duplicates(client) -> None:
    """Check for duplicate records in all tables."""
    logger.info("Checking for duplicate records...")
    
    # Queries to check for duplicates
    duplicate_checks = [
        {
            "name": "metadata_raw_series_id",
            "query": """
            SELECT 
                raw_series_id,
                count() as count
            FROM macro.economic_indicators_metadata
            GROUP BY raw_series_id
            HAVING count > 1
            ORDER BY count DESC
            LIMIT 5
            """
        },
        {
            "name": "values_series_date",
            "query": """
            SELECT 
                series_id,
                date,
                count() as count
            FROM macro.economic_indicators_values
            GROUP BY series_id, date
            HAVING count > 1
            ORDER BY count DESC
            LIMIT 5
            """
        },
        {
            "name": "monthly_series_date",
            "query": """
            SELECT 
                series_id,
                date,
                count() as count
            FROM macro.economic_indicators_monthly
            GROUP BY series_id, date
            HAVING count > 1
            ORDER BY count DESC
            LIMIT 5
            """
        },
        {
            "name": "changes_series_date",
            "query": """
            SELECT 
                series_id,
                date,
                count() as count
            FROM macro.economic_indicators_changes
            GROUP BY series_id, date
            HAVING count > 1
            ORDER BY count DESC
            LIMIT 5
            """
        }
    ]
    
    for check in duplicate_checks:
        try:
            result = client.query(check["query"])
            if result.result_rows:
                logger.warning(f"Found duplicates in {check['name']}:")
                for row in result.result_rows:
                    logger.warning(f"  {row}")
            else:
                logger.info(f"No duplicates found in {check['name']}")
        except Exception as e:
            logger.error(f"Error checking duplicates in {check['name']}: {str(e)}")
            raise

def check_query_performance(client) -> None:
    """Run example queries and measure their performance."""
    logger.info("Checking query performance...")
    
    # Example queries from example_queries.sql
    test_queries = [
        """
        SELECT 
            v.date,
            v.value,
            m.indicator_name,
            m.source,
            m.industry_name,
            m.seasonal_adjustment
        FROM macro.economic_indicators_values v
        JOIN macro.economic_indicators_metadata m ON v.series_id = m.series_id
        WHERE m.indicator_name LIKE '%manufacturing%'
        ORDER BY v.date DESC
        LIMIT 10
        """,
        """
        SELECT 
            m.indicator_name,
            m.industry_name,
            v.date,
            v.value
        FROM macro.economic_indicators_values v
        JOIN macro.economic_indicators_metadata m ON v.series_id = m.series_id
        WHERE v.date >= '2023-01-01'
        ORDER BY v.date DESC, m.indicator_name
        LIMIT 20
        """
    ]
    
    for i, query in enumerate(test_queries, 1):
        try:
            start_time = datetime.now()
            result = client.query(query)
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Query {i} executed in {execution_time:.2f} seconds")
            logger.info(f"Query {i} returned {len(result.result_rows)} rows")
        except Exception as e:
            logger.error(f"Error executing query {i}: {str(e)}")
            raise

def validate_data_quality(client):
    """Validate data quality in ClickHouse tables."""
    logger.info("Validating data quality...")
    
    # Check for missing values
    missing_values_query = """
    SELECT 
        series_id,
        COUNT(*) as total_rows,
        COUNT(CASE WHEN value IS NULL THEN 1 END) as null_values
    FROM macro.economic_indicators_values
    GROUP BY series_id
    HAVING null_values > 0
    """
    
    result = client.query(missing_values_query)
    if result.result_rows:
        logger.warning("Found series with missing values:")
        for row in result.result_rows:
            logger.warning(f"Series {row[0]}: {row[1]} total rows, {row[2]} null values")
    else:
        logger.info("No missing values found in the values table")
    
    # Check for annual averages
    annual_avg_query = """
    SELECT 
        m.indicator_name,
        COUNT(*) as total_rows,
        SUM(is_annual_avg) as annual_avg_rows
    FROM macro.economic_indicators_values v
    JOIN macro.economic_indicators_metadata m ON v.series_id = m.series_id
    GROUP BY m.indicator_name
    HAVING annual_avg_rows > 0
    """
    
    result = client.query(annual_avg_query)
    if result.result_rows:
        logger.info("Found indicators with annual averages:")
        for row in result.result_rows:
            logger.info(f"{row[0]}: {row[2]} annual averages out of {row[1]} total rows")
    else:
        logger.warning("No annual averages found in the values table")

def validate_materialized_views(client):
    """Validate materialized views data."""
    logger.info("Validating materialized views...")
    
    # Check monthly view
    monthly_query = """
    SELECT 
        m.indicator_name,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT date) AS unique_dates
    FROM macro.economic_indicators_monthly AS m
    GROUP BY m.indicator_name
    """
    result = client.query(monthly_query)
    logger.info(f"Monthly view validation result: {result}")
    
    # Check changes view
    changes_query = """
    SELECT 
        m.indicator_name,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT date) AS unique_dates
    FROM macro.economic_indicators_changes AS m
    GROUP BY m.indicator_name
    """
    result = client.query(changes_query)
    logger.info(f"Changes view validation result: {result}")
    
    # Check yearly view
    yearly_query = """
    SELECT 
        m.indicator_name,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT date) AS unique_dates
    FROM macro.economic_indicators_yearly AS m
    GROUP BY m.indicator_name
    """
    result = client.query(yearly_query)
    logger.info(f"Yearly view validation result: {result}")

def validate_annual_averages(client):
    """Validate annual averages by comparing ClickHouse calculations with BLS-provided values."""
    logger.info("Validating annual averages...")
    
    # Query to compare calculated vs provided annual averages
    annual_avg_query = """
    WITH bls_annual AS (
        SELECT 
            series_id,
            toStartOfYear(date) as year,
            value as bls_annual_value
        FROM macro.economic_indicators_values
        WHERE is_annual_avg = 1
    ),
    clickhouse_annual AS (
        SELECT 
            series_id,
            date as year,
            calculated_annual_value
        FROM macro.economic_indicators_yearly
    )
    SELECT 
        m.indicator_name,
        m.source,
        bls.year,
        bls.bls_annual_value,
        ch.calculated_annual_value,
        abs(bls.bls_annual_value - ch.calculated_annual_value) as difference,
        abs(bls.bls_annual_value - ch.calculated_annual_value) / bls.bls_annual_value * 100 as percent_difference
    FROM bls_annual bls
    JOIN clickhouse_annual ch ON bls.series_id = ch.series_id AND bls.year = ch.year
    JOIN macro.economic_indicators_metadata m ON bls.series_id = m.series_id
    WHERE abs(bls.bls_annual_value - ch.calculated_annual_value) > 0.0001  -- Allow for small floating point differences
    ORDER BY percent_difference DESC
    """
    
    result = client.query(annual_avg_query)
    if result.result_rows:
        logger.warning("Found discrepancies between BLS and ClickHouse annual averages:")
        for row in result.result_rows:
            logger.warning(
                f"{row[0]} ({row[1]}) - Year {row[2]}: "
                f"BLS={row[3]:.4f}, ClickHouse={row[4]:.4f}, "
                f"Diff={row[5]:.4f} ({row[6]:.2f}%)"
            )
    else:
        logger.info("All annual averages match between BLS and ClickHouse calculations")
    
    # Get summary statistics
    summary_query = """
    SELECT 
        COUNT(*) as total_comparisons,
        COUNT(CASE WHEN abs(bls.bls_annual_value - ch.calculated_annual_value) > 0.0001 THEN 1 END) as mismatches,
        AVG(abs(bls.bls_annual_value - ch.calculated_annual_value) / bls.bls_annual_value * 100) as avg_percent_difference
    FROM (
        SELECT 
            series_id,
            toStartOfYear(date) as year,
            value as bls_annual_value
        FROM macro.economic_indicators_values
        WHERE is_annual_avg = 1
    ) bls
    JOIN (
        SELECT 
            series_id,
            date as year,
            calculated_annual_value
        FROM macro.economic_indicators_yearly
    ) ch ON bls.series_id = ch.series_id AND bls.year = ch.year
    """
    
    result = client.query(summary_query)
    if result.result_rows:
        total, mismatches, avg_diff = result.result_rows[0]
        logger.info(f"Annual average validation summary:")
        logger.info(f"Total comparisons: {total}")
        logger.info(f"Mismatches: {mismatches}")
        logger.info(f"Average percent difference: {avg_diff:.4f}%")

def validate_clickhouse_data() -> None:
    """Main validation function."""
    logger.info("Starting ClickHouse data validation...")
    
    # Initialize ClickHouse client
    client = get_client(
        host='localhost',
        port=8123,
        username='default',
        password='clickhouse'
    )
    
    try:
        # Run all validation checks
        check_table_structure(client)
        check_data_completeness(client)
        check_materialized_views(client)
        check_duplicates(client)
        check_query_performance(client)
        validate_data_quality(client)
        validate_materialized_views(client)
        validate_annual_averages(client)
        
        logger.info("Validation completed successfully!")
        
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        raise
    finally:
        client.close()

if __name__ == "__main__":
    # Create logs directory if it doesn't exist
    os.makedirs("logs/validation", exist_ok=True)
    validate_clickhouse_data() 