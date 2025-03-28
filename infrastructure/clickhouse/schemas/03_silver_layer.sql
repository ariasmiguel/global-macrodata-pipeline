-- Silver Layer: Cleaned and standardized data
USE macro;

-- Series metadata table
CREATE TABLE IF NOT EXISTS silver_series_metadata
(
    series_id UUID DEFAULT generateUUIDv4(),
    raw_series_id String,
    source_id String,
    series_name String,
    series_type String,
    description String,
    frequency String,
    units String,
    seasonal_adjustment String,
    is_valid UInt8,
    processed_timestamp DateTime,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (raw_series_id);

-- Series values table
CREATE TABLE IF NOT EXISTS silver_series_values
(
    series_id UUID,
    raw_series_id String,
    year UInt16,
    quarter UInt8,
    value Float64,
    footnotes String,
    is_annual_avg UInt8,
    date Date,
    extraction_timestamp DateTime,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (series_id, date, raw_series_id); 