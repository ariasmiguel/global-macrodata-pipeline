-- Bronze Layer: Raw data storage
-- Store raw BLS data in its original form
USE macro;

-- Data source metadata table
CREATE TABLE IF NOT EXISTS bronze_data_sources
(
    source_id String,
    source_name String,
    source_type String,
    description String,
    api_endpoint String,
    api_version String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (source_id);

-- Store raw series metadata
CREATE TABLE IF NOT EXISTS bronze_series_metadata
(
    -- Core fields
    series_id String,
    series_name String,
    series_type LowCardinality(String),
    is_valid UInt8 DEFAULT 1,
    
    -- Raw data fields
    raw_data JSON, -- Store complete raw data as JSON
    raw_metadata JSON, -- Store additional metadata as JSON
    
    -- Technical fields
    processed_timestamp DateTime DEFAULT now(),
    source_file String
)
ENGINE = MergeTree()
ORDER BY series_id
SETTINGS index_granularity = 8192;

-- Store raw time series data
CREATE TABLE IF NOT EXISTS bronze_series_values
(
    -- Core fields
    series_id String,
    year UInt16,
    period String,
    period_name String,
    value String, -- Keep as string to preserve original format
    footnotes Array(JSON), -- Store footnotes as array of JSON objects
    
    -- Raw data fields
    raw_observation JSON, -- Store complete raw observation as JSON
    
    -- Technical fields
    extraction_timestamp DateTime DEFAULT now(),
    source_file String,
    
    -- Extract basic fields from JSON for better querying
    period_type LowCardinality(String) MATERIALIZED multiIf(
        startsWith(period, 'M'), 'monthly',
        startsWith(period, 'Q'), 'quarterly',
        startsWith(period, 'A'), 'annual',
        'other'
    )
)
ENGINE = MergeTree()
PARTITION BY year
ORDER BY (series_id, year, period)
SETTINGS index_granularity = 8192;

-- Insert default data source
INSERT INTO bronze_data_sources 
(source_id, source_name, source_type, description, api_endpoint, api_version)
VALUES 
('BLS_API', 'Bureau of Labor Statistics', 'API', 
 'Official source for Bureau of Labor Statistics data', 
 'https://api.bls.gov/publicAPI/v2/timeseries/data/', 
 'v2'); 