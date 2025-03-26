from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Configure logging
logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """Base class for all data extractors."""
    
    def __init__(self, headers: Optional[Dict[str, str]] = None):
        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        logger.debug("Initialized BaseExtractor with headers: %s", self.headers)
        
    def _get_selenium_driver(self) -> webdriver.Chrome:
        """Initialize headless Chrome driver."""
        logger.debug("Initializing headless Chrome driver")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        return webdriver.Chrome(options=chrome_options)
    
    @abstractmethod
    def extract(self) -> Any:
        """Extract data from source."""
        pass
    
    def __enter__(self):
        logger.debug("Entering context manager")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Exiting context manager")
        self.session.close() 