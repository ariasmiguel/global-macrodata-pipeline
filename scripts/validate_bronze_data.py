import logging
from pathlib import Path
import json
from typing import Dict, List, Tuple
from datetime import datetime
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BronzeDataValidator:
    """Validator for BLS API raw data in the bronze layer."""
    
    def __init__(self, bronze_dir: Path = Path("data/bronze/raw_series_data")):
        self.bronze_dir = bronze_dir
        self.validation_results = {
            'total_files': 0,
            'valid_files': 0,
            'invalid_files': 0,
            'errors': [],
            'series_stats': {},
            'data_quality': {}
        }

    def validate_json_structure(self, data: Dict) -> Tuple[bool, List[str]]:
        """Validate the basic JSON structure of a BLS API response."""
        errors = []
        
        # Required fields
        if 'seriesID' not in data:
            errors.append("Missing seriesID field")
        
        if 'data' not in data:
            errors.append("Missing data field")
            return False, errors
            
        if not isinstance(data['data'], list):
            errors.append("Data field is not a list")
            return False, errors
            
        # Validate data points structure
        for idx, point in enumerate(data['data']):
            point_errors = []
            required_fields = ['year', 'period', 'periodName', 'value']
            
            for field in required_fields:
                if field not in point:
                    point_errors.append(f"Missing {field}")
                    
            if point_errors:
                errors.append(f"Data point {idx} errors: {', '.join(point_errors)}")
                
            # Validate data types
            try:
                int(point['year'])
                float(point['value'])
                if not point['period'].startswith('M') or not (1 <= int(point['period'][1:]) <= 12):
                    point_errors.append("Invalid period format (should be M01-M12)")
            except (ValueError, IndexError):
                errors.append(f"Data point {idx} has invalid numeric values")
                
        return len(errors) == 0, errors

    def validate_time_series(self, data: Dict) -> Tuple[bool, List[str]]:
        """Validate time series consistency and completeness."""
        errors = []
        
        # Sort data points by year and period
        try:
            sorted_data = sorted(
                data['data'], 
                key=lambda x: (int(x['year']), int(x['period'][1:]))
            )
            
            # Check for duplicates
            time_points = [(d['year'], d['period']) for d in sorted_data]
            if len(time_points) != len(set(time_points)):
                errors.append("Contains duplicate time points")
            
            # Check for gaps in monthly data
            for i in range(len(sorted_data) - 1):
                current = sorted_data[i]
                next_point = sorted_data[i + 1]
                
                current_date = datetime.strptime(f"{current['year']}-{current['period'][1:]}", "%Y-%m")
                next_date = datetime.strptime(f"{next_point['year']}-{next_point['period'][1:]}", "%Y-%m")
                
                month_diff = (next_date.year - current_date.year) * 12 + next_date.month - current_date.month
                if month_diff > 1:
                    errors.append(f"Gap in time series between {current['year']}-{current['period']} and {next_point['year']}-{next_point['period']}")
                    
        except (ValueError, KeyError) as e:
            errors.append(f"Error processing time series: {str(e)}")
            
        return len(errors) == 0, errors

    def validate_value_ranges(self, data: Dict) -> Tuple[bool, List[str]]:
        """Validate value ranges and detect potential anomalies."""
        errors = []
        values = [float(point['value']) for point in data['data']]
        
        if not values:
            errors.append("No values found in series")
            return False, errors
            
        # Basic statistics
        series_stats = {
            'min': min(values),
            'max': max(values),
            'mean': sum(values) / len(values),
            'count': len(values)
        }
        
        # Check for potential anomalies
        if series_stats['count'] > 0:
            # Calculate standard deviation
            mean = series_stats['mean']
            std_dev = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5
            
            # Flag values more than 3 standard deviations from mean
            outliers = [
                (point['year'], point['period'], float(point['value']))
                for point in data['data']
                if abs(float(point['value']) - mean) > 3 * std_dev
            ]
            
            if outliers:
                errors.append(f"Found {len(outliers)} potential outliers: {outliers}")
                
        # Store statistics for reporting
        self.validation_results['series_stats'][data['seriesID']] = series_stats
        
        return len(errors) == 0, errors

    def validate_file(self, file_path: Path) -> Tuple[bool, List[str]]:
        """Validate a single JSON file."""
        errors = []
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                
            # Run all validations
            structure_valid, structure_errors = self.validate_json_structure(data)
            time_valid, time_errors = self.validate_time_series(data)
            values_valid, value_errors = self.validate_value_ranges(data)
            
            errors.extend(structure_errors)
            errors.extend(time_errors)
            errors.extend(value_errors)
            
            return len(errors) == 0, errors
            
        except json.JSONDecodeError as e:
            return False, [f"Invalid JSON format: {str(e)}"]
        except Exception as e:
            return False, [f"Unexpected error: {str(e)}"]

    def validate_all_files(self) -> Dict:
        """Validate all JSON files in the bronze directory."""
        for file_path in self.bronze_dir.glob("*.json"):
            if file_path.name == "extraction_results.json":
                continue
                
            self.validation_results['total_files'] += 1
            valid, errors = self.validate_file(file_path)
            
            if valid:
                self.validation_results['valid_files'] += 1
            else:
                self.validation_results['invalid_files'] += 1
                self.validation_results['errors'].append({
                    'file': file_path.name,
                    'errors': errors
                })
                
        return self.validation_results

    def generate_validation_report(self) -> None:
        """Generate a detailed validation report."""
        report_path = self.bronze_dir / "validation_report.json"
        
        # Add timestamp to report
        self.validation_results['timestamp'] = datetime.now().isoformat()
        
        # Calculate success rate
        total = self.validation_results['total_files']
        if total > 0:
            success_rate = (self.validation_results['valid_files'] / total) * 100
            self.validation_results['success_rate'] = f"{success_rate:.2f}%"
        
        # Save report
        with open(report_path, 'w') as f:
            json.dump(self.validation_results, f, indent=2)
            
        logger.info(f"Validation report saved to {report_path}")

def main():
    """Main function to run bronze layer validation."""
    try:
        start_time = datetime.now()
        logger.info("Starting bronze layer data validation...")
        
        validator = BronzeDataValidator()
        results = validator.validate_all_files()
        validator.generate_validation_report()
        
        # Log summary
        logger.info("\nValidation Summary:")
        logger.info(f"Total files processed: {results['total_files']}")
        logger.info(f"Valid files: {results['valid_files']}")
        logger.info(f"Invalid files: {results['invalid_files']}")
        logger.info(f"Success rate: {results.get('success_rate', '0%')}")
        
        if results['errors']:
            logger.warning(f"\nFound {len(results['errors'])} files with errors")
            
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"\nTotal execution time: {execution_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main() 