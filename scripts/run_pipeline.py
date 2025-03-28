"""
Main script to run the complete Macroeconomic Data Pipeline.
This script orchestrates the execution of all steps in the pipeline in the correct order.
"""

import os
import sys
import logging
import time
import subprocess
import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple

# Configure logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'logs/pipeline_run_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

class PipelineRunner:
    """Class to run the macroeconomic data pipeline."""
    
    def __init__(self):
        self.start_time = time.time()
        self.results = {}
        
        # Define the pipeline steps with their scripts and descriptions
        self.pipeline_steps = get_pipeline_steps()
    
    def run_script(self, script_path: str, step_num: int, description: str) -> Tuple[bool, str, float]:
        """Run a Python script and return the result."""
        logger.info(f"Step {step_num}: {description}")
        logger.info(f"Running script: {script_path}")
        
        step_start_time = time.time()
        
        try:
            # Run the script as a subprocess
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                check=False  # Don't raise exception on non-zero exit code
            )
            
            # Check if the script ran successfully
            if result.returncode == 0:
                logger.info(f"Step {step_num} completed successfully")
                success = True
            else:
                logger.error(f"Step {step_num} failed with exit code {result.returncode}")
                logger.error(f"Error output: {result.stderr}")
                success = False
            
            # Log stdout regardless of success
            if result.stdout:
                for line in result.stdout.splitlines():
                    logger.debug(f"[Step {step_num} output] {line}")
            
            duration = time.time() - step_start_time
            logger.info(f"Step {step_num} took {duration:.2f} seconds")
            
            return success, result.stderr if not success else "", duration
            
        except Exception as e:
            logger.error(f"Exception running step {step_num}: {str(e)}")
            return False, str(e), time.time() - step_start_time
    
    def run_pipeline(self, start_step: int = 1, end_step: int = None) -> Dict[str, Any]:
        """Run the pipeline steps from start_step to end_step (inclusive)."""
        logger.info("Starting Macroeconomic Data Pipeline")
        logger.info(f"Running steps {start_step} to {end_step or 'end'}")
        
        if end_step is None:
            end_step = len(self.pipeline_steps)
        
        # Filter steps to run
        steps_to_run = [
            step for step in self.pipeline_steps
            if start_step <= step["step"] <= end_step
        ]
        
        if not steps_to_run:
            logger.error(f"No steps to run between {start_step} and {end_step}")
            return {"status": "error", "message": "No steps to run"}
        
        # Initialize results
        pipeline_results = {
            "start_time": datetime.datetime.now().isoformat(),
            "steps": {}
        }
        
        # Run each step in sequence
        all_successful = True
        
        for step in steps_to_run:
            step_num = step["step"]
            script = step["script"]
            description = step["description"]
            
            # Check if script exists
            if not Path(script).exists():
                logger.error(f"Script not found: {script}")
                pipeline_results["steps"][str(step_num)] = {
                    "status": "error",
                    "message": "Script not found",
                    "duration_seconds": 0
                }
                all_successful = False
                # Continue to next step or break the pipeline?
                # For now, we'll continue to report all missing scripts
                continue
            
            # Run the script
            success, error, duration = self.run_script(script, step_num, description)
            
            # Record results
            pipeline_results["steps"][str(step_num)] = {
                "script": script,
                "description": description,
                "status": "success" if success else "error",
                "error_message": error,
                "duration_seconds": round(duration, 2)
            }
            
            # If a step fails, stop the pipeline unless configured otherwise
            if not success:
                logger.error(f"Pipeline stopping due to failure in step {step_num}")
                all_successful = False
                break
        
        # Calculate total time
        end_time = time.time()
        total_duration = end_time - self.start_time
        
        # Add summary to results
        pipeline_results["end_time"] = datetime.datetime.now().isoformat()
        pipeline_results["total_duration_seconds"] = round(total_duration, 2)
        pipeline_results["status"] = "success" if all_successful else "error"
        
        # Log summary
        logger.info("====== Pipeline Execution Summary ======")
        logger.info(f"Status: {pipeline_results['status']}")
        logger.info(f"Total execution time: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
        
        for step_num, result in pipeline_results["steps"].items():
            status_icon = "✅" if result["status"] == "success" else "❌"
            logger.info(f"Step {step_num}: {status_icon} {result['description']} - {result['duration_seconds']:.2f}s")
        
        logger.info("=======================================")
        
        # Save results to file
        results_dir = Path("data/logs")
        results_dir.mkdir(parents=True, exist_ok=True)
        
        import json
        results_file = results_dir / f"pipeline_results_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, "w") as f:
            json.dump(pipeline_results, f, indent=2)
        
        logger.info(f"Pipeline results saved to {results_file}")
        
        return pipeline_results

def get_pipeline_steps():
    """Get the list of pipeline steps to execute."""
    return [
        {
            "step": 1,
            "description": "Initialize ClickHouse database with schema",
            "script": "scripts/setup/setup_clickhouse.py"
        },
        {
            "step": 2,
            "description": "Extract data from BLS API",
            "script": "scripts/ingestion/02_extract_bls_data.py"
        },
        {
            "step": 3,
            "description": "Load raw data into ClickHouse bronze layer",
            "script": "scripts/ingestion/03_load_bronze_to_clickhouse.py"
        },
        {
            "step": 4,
            "description": "Transform data to silver layer",
            "script": "scripts/cleaning/04_transform_to_silver.py"
        },
        {
            "step": 5,
            "description": "Load cleaned data to silver layer",
            "script": "scripts/ingestion/05_load_silver_to_clickhouse.py"
        },
        {
            "step": 6,
            "description": "Transform data to gold layer",
            "script": "scripts/cleaning/06_transform_to_gold.py"
        },
        {
            "step": 7,
            "description": "Load and validate gold layer data",
            "script": "scripts/ingestion/07_load_gold_to_clickhouse.py"
        },
        {
            "step": 8,
            "description": "Generate dashboard data",
            "script": "scripts/analytics/08_generate_dashboard_data.py"
        }
    ]

def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run the Macroeconomic Data Pipeline')
    parser.add_argument('--start-step', type=int, default=1, help='Step to start from (default: 1)')
    parser.add_argument('--end-step', type=int, default=None, help='Step to end with (default: run all steps)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Set debug level if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Initialize and run the pipeline
    runner = PipelineRunner()
    results = runner.run_pipeline(
        start_step=args.start_step,
        end_step=args.end_step
    )
    
    # Exit with appropriate code
    sys.exit(0 if results["status"] == "success" else 1)

if __name__ == "__main__":
    main() 