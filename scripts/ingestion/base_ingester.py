from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pathlib import Path

import pandas as pd
from clickhouse_driver import Client


class BaseIngester(ABC):
    """Base class for all data ingesters."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.client = None
    
    def _connect_clickhouse(self, host: str = 'localhost', port: int = 9000):
        """Connect to Clickhouse database."""
        self.client = Client(host=host, port=port)
    
    def _save_to_parquet(self, data: pd.DataFrame, path: Path):
        """Save DataFrame to parquet file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(path)
    
    @abstractmethod
    def ingest(self, data: Any) -> None:
        """Ingest data into target system."""
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.disconnect() 