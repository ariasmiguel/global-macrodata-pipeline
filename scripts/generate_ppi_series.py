from pathlib import Path
from macrodata_pipeline.extractors.bls import BLSExtractor

def main():
    # Initialize paths
    data_dir = Path("data")
    bronze_dir = data_dir / "bronze"
    
    industry_codes_path = bronze_dir / "ppi_industry_codes.csv"
    product_codes_path = bronze_dir / "ppi_product_codes.csv"
    output_path = bronze_dir / "ppi_series_ids.txt"
    
    # Initialize extractor
    extractor = BLSExtractor()
    
    # Generate series IDs
    print("Generating PPI series IDs...")
    series_ids = extractor.generate_ppi_series_ids(
        industry_codes_path=industry_codes_path,
        product_codes_path=product_codes_path
    )
    print(f"Generated {len(series_ids)} potential series IDs")
    
    # Validate series IDs
    print("\nValidating series IDs (this may take a while)...")
    valid_series = extractor.validate_ppi_series(series_ids)
    print(f"Found {len(valid_series)} valid series IDs")
    
    # Save valid series IDs
    extractor.save_ppi_series_ids(valid_series, output_path)
    print(f"\nSaved valid series IDs to {output_path}")
    
    # Print sample of valid series
    print("\nSample of valid series IDs:")
    for series_id in valid_series[:5]:
        print(series_id)

if __name__ == "__main__":
    main() 