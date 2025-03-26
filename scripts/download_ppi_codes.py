import logging
from pathlib import Path
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import io

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_raw_data(content):
    """Clean raw data by removing HTML tags and extra whitespace."""
    # Remove HTML tags if present
    if '<html>' in content:
        content = content.split('<pre style="word-wrap: break-word; white-space: pre-wrap;">')[-1]
        content = content.split('</pre>')[0]
    
    # Clean up the content
    lines = content.strip().split('\n')
    cleaned_lines = [line.strip() for line in lines if line.strip()]
    return '\n'.join(cleaned_lines)

def download_ppi_codes():
    """Download PPI industry and product codes directly from BLS data files."""
    # Create directories
    bronze_dir = Path("data/bronze")
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    # BLS PPI data URLs
    urls = {
        'industry': "https://download.bls.gov/pub/time.series/pc/pc.industry",
        'product': "https://download.bls.gov/pub/time.series/pc/pc.product"
    }
    
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
        wait = WebDriverWait(driver, timeout=10)
        
        # Download and process each file
        for code_type, url in urls.items():
            logger.info(f"Downloading {code_type} codes from {url}")
            
            try:
                # First visit the BLS homepage to set cookies
                driver.get("https://www.bls.gov/")
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                
                # Now visit the actual data URL
                driver.get(url)
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
                
                # Get and clean the page source
                content = driver.page_source
                cleaned_content = clean_raw_data(content)
                
                # Read the tab-delimited file
                df = pd.read_csv(io.StringIO(cleaned_content), sep='\t')
                
                # Clean up column names and data
                df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                df.columns = df.columns.str.strip().str.lower()
                
                # Save to CSV
                output_path = bronze_dir / f"ppi_{code_type}_codes.csv"
                df.to_csv(output_path, index=False)
                
                logger.info(f"Saved {len(df)} {code_type} codes to {output_path}")
                logger.info(f"Columns: {', '.join(df.columns)}")
                logger.info(f"Sample {code_type} codes:\n{df.head().to_string()}")
                
            except Exception as e:
                logger.error(f"Error processing {code_type} codes: {str(e)}")
                raise
            
    except Exception as e:
        logger.error(f"Error downloading PPI codes: {str(e)}")
        raise
    finally:
        if 'driver' in locals():
            driver.quit()

if __name__ == "__main__":
    download_ppi_codes() 