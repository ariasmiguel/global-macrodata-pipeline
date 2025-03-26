import logging
from pathlib import Path
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
from typing import List, Dict, Set, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BLSSeriesExtractor:
    def __init__(self):
        self.series_patterns = {
            'CPI': r'C[UW][0-9]{2}[0-9A-Z]{2}[0-9A-Z]',  # Consumer Price Index
            'PPI': r'PCU[0-9]{6}',  # Producer Price Index
            'CES': r'CE[0-9]{8}',  # Current Employment Statistics
            'JOLTS': r'JT[0-9]{8}',  # Job Openings and Labor Turnover Survey
            'LAUS': r'LA[0-9]{8}',  # Local Area Unemployment Statistics
            'OES': r'OE[0-9]{8}',  # Occupational Employment Statistics
            'QCEW': r'EN[0-9]{8}',  # Quarterly Census of Employment and Wages
        }
        
        self.series_components = {
            'CPI': {
                'prefix': ['CU', 'CW'],
                'seasonal': ['S', 'U'],
                'area': ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10'],
                'item': ['SA0', 'SA1', 'SA2', 'SA3', 'SA4', 'SA5', 'SA6', 'SA7', 'SA8', 'SA9'],
                'period': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']
            },
            'PPI': {
                'prefix': ['PCU'],
                'industry': [],  # Will be populated from PPI codes
                'product': [],   # Will be populated from PPI codes
                'period': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']
            }
        }

    def clean_text(self, text: str) -> str:
        """Clean up text by removing HTML and special characters."""
        # Remove HTML tags
        text = BeautifulSoup(text, 'html.parser').get_text()
        # Remove tabs and multiple spaces
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing whitespace
        text = text.strip()
        return text

    def extract_series_examples(self, page_source: str) -> Dict[str, List[str]]:
        """Extract series examples from the page source."""
        soup = BeautifulSoup(page_source, 'html.parser')
        examples = {}
        
        # Find all pre-formatted text blocks
        pre_blocks = soup.find_all('pre')
        for block in pre_blocks:
            text = block.get_text()
            for series_type, pattern in self.series_patterns.items():
                matches = re.finditer(pattern, text)
                if series_type not in examples:
                    examples[series_type] = set()
                for match in matches:
                    examples[series_type].add(match.group(0))
        
        # Convert sets to sorted lists
        return {k: sorted(list(v)) for k, v in examples.items()}

    def load_ppi_codes(self) -> Tuple[List[str], List[str]]:
        """Load PPI industry and product codes from CSV files."""
        bronze_dir = Path("data/bronze")
        
        industry_df = pd.read_csv(bronze_dir / "ppi_industry_codes.csv")
        product_df = pd.read_csv(bronze_dir / "ppi_product_codes.csv")
        
        return industry_df['code'].tolist(), product_df['code'].tolist()

    def generate_series_combinations(self) -> Dict[str, List[str]]:
        """Generate all possible series combinations."""
        # Load PPI codes
        industry_codes, product_codes = self.load_ppi_codes()
        self.series_components['PPI']['industry'] = industry_codes
        self.series_components['PPI']['product'] = product_codes
        
        combinations = {}
        
        # Generate CPI series
        cpi_series = []
        for prefix in self.series_components['CPI']['prefix']:
            for seasonal in self.series_components['CPI']['seasonal']:
                for area in self.series_components['CPI']['area']:
                    for item in self.series_components['CPI']['item']:
                        for period in self.series_components['CPI']['period']:
                            series_id = f"{prefix}{seasonal}{area}{item}{period}"
                            cpi_series.append(series_id)
        combinations['CPI'] = cpi_series
        
        # Generate PPI series
        ppi_series = []
        for prefix in self.series_components['PPI']['prefix']:
            for industry in self.series_components['PPI']['industry']:
                for product in self.series_components['PPI']['product']:
                    for period in self.series_components['PPI']['period']:
                        series_id = f"{prefix}{industry}{product}{period}"
                        ppi_series.append(series_id)
        combinations['PPI'] = ppi_series
        
        return combinations

    def download_and_extract(self):
        """Download BLS documentation and extract series information."""
        # Create directories
        bronze_dir = Path("data/bronze")
        bronze_dir.mkdir(parents=True, exist_ok=True)
        
        # BLS format documentation URL
        url = "https://www.bls.gov/help/hlpforma.htm"
        
        # Configure Chrome options
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36")
        
        try:
            logger.info("Setting up Chrome driver...")
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            logger.info(f"Accessing {url}...")
            driver.get(url)
            
            # Wait for the page to load
            time.sleep(5)
            
            # Get the page source
            page_source = driver.page_source
            
            # Extract series examples
            examples = self.extract_series_examples(page_source)
            
            # Generate all possible combinations
            combinations = self.generate_series_combinations()
            
            # Save results
            for series_type, series_list in combinations.items():
                df = pd.DataFrame({'series_id': series_list})
                output_path = bronze_dir / f"bls_{series_type.lower()}_series.csv"
                df.to_csv(output_path, index=False)
                logger.info(f"Saved {len(series_list)} {series_type} series to {output_path}")
                logger.info(f"Sample {series_type} series:\n{df.head()}")
            
            # Save examples
            examples_df = pd.DataFrame({
                'series_type': [k for k, v in examples.items() for _ in v],
                'series_id': [s for v in examples.values() for s in v]
            })
            examples_path = bronze_dir / "bls_series_examples.csv"
            examples_df.to_csv(examples_path, index=False)
            logger.info(f"Saved {len(examples_df)} example series to {examples_path}")
            
        except Exception as e:
            logger.error(f"Error extracting BLS series: {str(e)}")
            raise
        finally:
            if 'driver' in locals():
                driver.quit()

if __name__ == "__main__":
    extractor = BLSSeriesExtractor()
    extractor.download_and_extract() 