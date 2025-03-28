"""ClickHouse database connection utilities.

This module provides utilities for connecting to and interacting with ClickHouse databases.
It includes connection management, query execution, and error handling.
"""

import os
import logging
from typing import Optional, Any, Dict, List
from pathlib import Path
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent.parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

logger = logging.getLogger(__name__)

class ClickHouseClient:
    """A wrapper class for ClickHouse database connections."""
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None
    ):
        """Initialize ClickHouse client with connection parameters.
        
        Args:
            host: ClickHouse server host (default: from CLICKHOUSE_HOST env var)
            port: ClickHouse server port (default: from CLICKHOUSE_PORT env var)
            username: Database username (default: from CLICKHOUSE_USER env var)
            password: Database password (default: from CLICKHOUSE_PASSWORD env var)
            database: Database name (default: from CLICKHOUSE_DB env var)
        """
        # Get credentials from environment variables
        self.host = host or os.environ.get('CLICKHOUSE_HOST')
        self.port = port or int(os.environ.get('CLICKHOUSE_PORT', '8123'))
        self.username = username or os.environ.get('CLICKHOUSE_USER')
        self.password = password or os.environ.get('CLICKHOUSE_PASSWORD')
        self.database = database or os.environ.get('CLICKHOUSE_DB', 'macro')
        
        # Validate required credentials
        self._validate_credentials()
        
        self._client: Optional[Client] = None
    
    def _validate_credentials(self) -> None:
        """Validate that all required credentials are present."""
        missing = []
        if not self.host:
            missing.append('CLICKHOUSE_HOST')
        if not self.username:
            missing.append('CLICKHOUSE_USER')
        if not self.password:
            missing.append('CLICKHOUSE_PASSWORD')
            
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}. "
                "Please ensure these are set in your .env file."
            )
        
    def connect(self) -> None:
        """Establish connection to ClickHouse database."""
        try:
            # Log connection attempt without sensitive details
            logger.info(f"Connecting to ClickHouse database: {self.database}")
            
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database
            )
            
            # Test connection
            version = self._client.query("SELECT version()").first_row[0]
            logger.info(f"Successfully connected to ClickHouse version {version}")
            
        except Exception as e:
            # Log error without exposing sensitive details
            logger.error(f"Failed to connect to ClickHouse database: {str(e)}")
            raise
    
    def close(self) -> None:
        """Close the database connection."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Closed ClickHouse connection")
    
    def execute_query(self, query: str, parameters: Dict[str, Any] = None) -> List[Any]:
        """Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute
            parameters: Query parameters (optional)
            
        Returns:
            List of query results
        """
        if not self._client:
            self.connect()
            
        try:
            result = self._client.query(query, parameters=parameters)
            return result.result_rows
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def insert_dataframe(self, table: str, df: Any) -> int:
        """Insert a pandas DataFrame into a ClickHouse table.
        
        Args:
            table: Target table name
            df: pandas DataFrame to insert
            
        Returns:
            Number of rows inserted
        """
        if not self._client:
            self.connect()
            
        try:
            self._client.insert_df(table, df)
            return len(df)
        except Exception as e:
            logger.error(f"Error inserting DataFrame into {table}: {str(e)}")
            raise
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

def get_clickhouse_client(
    host: Optional[str] = None,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None
) -> ClickHouseClient:
    """Factory function to create a ClickHouse client.
    
    Args:
        host: ClickHouse server host (default: from CLICKHOUSE_HOST env var)
        port: ClickHouse server port (default: from CLICKHOUSE_PORT env var)
        username: Database username (default: from CLICKHOUSE_USER env var)
        password: Database password (default: from CLICKHOUSE_PASSWORD env var)
        database: Database name (default: from CLICKHOUSE_DB env var)
        
    Returns:
        ClickHouseClient instance
    """
    return ClickHouseClient(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database
    ) 