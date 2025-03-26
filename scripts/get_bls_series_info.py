from pathlib import Path
from macrodata_pipeline.extractors.bls import BLSExtractor

def main():
    # Initialize extractor
    extractor = BLSExtractor()
    
    # Get all series information
    df = extractor.get_all_series_info()
    
    # Save to bronze layer
    output_path = Path("data/bronze/bls_series_info.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path)
    
    print(f"Series information saved to {output_path}")
    print(f"Total number of series: {len(df)}")
    print("\nSample data:")
    print(df.head())
    
    # Print unique survey names
    print("\nAvailable surveys:")
    print(df["survey_name"].unique())

if __name__ == "__main__":
    main() 