from pathlib import Path
from bls_extractor import BLSExtractor

def main():
    # Example BLS series IDs
    series_ids = [
        "CUSR0000SA0",  # CPI for All Urban Consumers: All Items
        "CEU0000000001"  # Total Nonfarm Employment
    ]
    
    # Initialize extractor
    extractor = BLSExtractor()
    
    # Extract data for the last 5 years
    df = extractor.extract(
        series_ids=series_ids,
        start_year=2019,
        end_year=2024
    )
    
    # Save to bronze layer
    output_path = Path("data/bronze/bls_data.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path)
    
    print(f"Data extracted and saved to {output_path}")
    print(f"Shape: {df.shape}")
    print("\nSample data:")
    print(df.head())

if __name__ == "__main__":
    main() 