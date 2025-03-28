"""Macroeconomic Data Pipeline package.

This package provides utilities for extracting, transforming, and loading
macroeconomic data from various sources into a data warehouse.
"""

from macrodata_pipeline.database import ClickHouseClient, get_clickhouse_client
from macrodata_pipeline.utils import get_logger, set_log_level

__version__ = '0.1.0'

__all__ = [
    'ClickHouseClient',
    'get_clickhouse_client',
    'get_logger',
    'set_log_level'
] 