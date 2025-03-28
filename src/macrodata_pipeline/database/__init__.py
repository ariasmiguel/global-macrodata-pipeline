"""Database package for the Macroeconomic Data Pipeline."""

from macrodata_pipeline.database.clickhouse import ClickHouseClient, get_clickhouse_client

__all__ = ['ClickHouseClient', 'get_clickhouse_client'] 