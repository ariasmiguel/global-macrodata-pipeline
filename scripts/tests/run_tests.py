#!/usr/bin/env python3
"""
Test runner script for the Macroeconomic Data Pipeline.
This script runs all test files in the tests directory and reports results.
"""

import os
import sys
import subprocess
import glob
from pathlib import Path
import time
import datetime
import logging

# Configure logging
os.makedirs("logs/tests", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'logs/tests/test_run_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

def run_tests():
    """Run all test files and report results."""
    start_time = time.time()
    logger.info("Starting test run")
    
    # Get all test files
    script_dir = Path(__file__).parent
    test_files = sorted(script_dir.glob("test_*.py"))
    
    if not test_files:
        logger.error("No test files found")
        return False
    
    logger.info(f"Found {len(test_files)} test files")
    
    # Run each test
    results = {}
    success_count = 0
    
    for test_file in test_files:
        test_name = test_file.name
        logger.info(f"Running {test_name}...")
        
        test_start_time = time.time()
        
        try:
            # Run the test script
            result = subprocess.run(
                [sys.executable, str(test_file)],
                capture_output=True,
                text=True,
                check=False
            )
            
            success = result.returncode == 0
            if success:
                logger.info(f"✅ {test_name} passed")
                success_count += 1
            else:
                logger.error(f"❌ {test_name} failed with exit code {result.returncode}")
                if result.stderr:
                    logger.error(f"Error output: {result.stderr}")
            
            # Log stdout regardless of success
            if result.stdout:
                logger.debug(f"Output from {test_name}:")
                for line in result.stdout.splitlines():
                    logger.debug(f"  {line}")
            
            duration = time.time() - test_start_time
            results[test_name] = {
                "status": "passed" if success else "failed",
                "duration_seconds": round(duration, 2),
                "exit_code": result.returncode
            }
            
            logger.info(f"Test {test_name} took {duration:.2f} seconds")
            logger.info("-" * 40)
            
        except Exception as e:
            logger.error(f"Error running {test_name}: {str(e)}")
            results[test_name] = {
                "status": "error",
                "duration_seconds": round(time.time() - test_start_time, 2),
                "error": str(e)
            }
    
    # Calculate total time
    total_duration = time.time() - start_time
    
    # Log summary
    logger.info("====== Test Execution Summary ======")
    logger.info(f"Total tests: {len(test_files)}")
    logger.info(f"Passed: {success_count}")
    logger.info(f"Failed: {len(test_files) - success_count}")
    logger.info(f"Total execution time: {total_duration:.2f} seconds")
    
    for test_name, result in results.items():
        status_icon = "✅" if result["status"] == "passed" else "❌"
        logger.info(f"{status_icon} {test_name} - {result['duration_seconds']}s")
    
    logger.info("===================================")
    
    return success_count == len(test_files)

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1) 