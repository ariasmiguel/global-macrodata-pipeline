-- Create dimension table for economic indicators metadata
CREATE TABLE IF NOT EXISTS macro.economic_indicators_metadata
(
    -- Primary key
    series_id UUID DEFAULT generateUUIDv4(),
    
    -- Source reference
    raw_series_id String,
    
    -- Core metadata
    indicator_name String,  -- Not LowCardinality as it might have >10k unique values
    source LowCardinality(String),
    
    -- Additional metadata
    industry_code LowCardinality(String),
    industry_name LowCardinality(String),
    series_type LowCardinality(String),
    seasonal_adjustment LowCardinality(String),
    base_period LowCardinality(String),
    
    -- Technical fields
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY series_id
SETTINGS index_granularity = 8192;

-- Create fact table for economic indicators values
CREATE TABLE IF NOT EXISTS macro.economic_indicators_values
(
    -- Foreign key
    series_id UUID,
    
    -- Core fields
    date Date,
    value Float64,
    is_annual_avg UInt8 DEFAULT 0,  -- 0 = false, 1 = true
    
    -- Technical fields
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (series_id, date)
SETTINGS index_granularity = 8192;

-- Create materialized view for monthly aggregations with metadata
CREATE MATERIALIZED VIEW IF NOT EXISTS macro.economic_indicators_monthly
ENGINE = AggregatingMergeTree()
PARTITION BY toYear(date)
ORDER BY (series_id, date)
AS SELECT
    v.series_id,
    m.raw_series_id,
    m.source,
    m.indicator_name,
    toStartOfMonth(v.date) as date,
    finalizeAggregation(avgState(v.value)) as avg_value,
    finalizeAggregation(minState(v.value)) as min_value,
    finalizeAggregation(maxState(v.value)) as max_value
FROM macro.economic_indicators_values v
JOIN macro.economic_indicators_metadata m ON v.series_id = m.series_id
WHERE v.is_annual_avg = 0  -- Only include non-annual values
GROUP BY v.series_id, m.raw_series_id, m.source, m.indicator_name, toStartOfMonth(v.date);

-- Create materialized view for YoY and MoM changes with metadata
CREATE MATERIALIZED VIEW IF NOT EXISTS macro.economic_indicators_changes
ENGINE = AggregatingMergeTree()
PARTITION BY toYear(date)
ORDER BY (series_id, date)
AS SELECT
    v.series_id,
    m.raw_series_id,
    m.source,
    m.indicator_name,
    v.date,
    v.value,
    v.value / lagInFrame(v.value) OVER (PARTITION BY v.series_id ORDER BY v.date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) - 1 as mom_change,
    v.value / lagInFrame(v.value) OVER (PARTITION BY v.series_id ORDER BY v.date ROWS BETWEEN 12 PRECEDING AND 12 PRECEDING) - 1 as yoy_change
FROM macro.economic_indicators_values v
JOIN macro.economic_indicators_metadata m ON v.series_id = m.series_id
WHERE v.is_annual_avg = 0;  -- Only include non-annual values

-- Create materialized view for yearly aggregations (including annual averages)
-- Note: This view calculates annual averages from monthly values
-- The validation script will compare these calculated averages with the BLS-provided annual averages
CREATE MATERIALIZED VIEW IF NOT EXISTS macro.economic_indicators_yearly
ENGINE = AggregatingMergeTree()
PARTITION BY toYear(date)
ORDER BY (series_id, date)
AS SELECT
    v.series_id,
    m.raw_series_id,
    m.source,
    m.indicator_name,
    toStartOfYear(v.date) as date,
    finalizeAggregation(avgState(v.value)) as avg_value,
    finalizeAggregation(minState(v.value)) as min_value,
    finalizeAggregation(maxState(v.value)) as max_value,
    -- Calculate annual average from monthly values
    finalizeAggregation(avgState(v.value)) as calculated_annual_value
FROM macro.economic_indicators_values v
JOIN macro.economic_indicators_metadata m ON v.series_id = m.series_id
WHERE v.is_annual_avg = 0  -- Only use monthly values for calculations
GROUP BY v.series_id, m.raw_series_id, m.source, m.indicator_name, toStartOfYear(v.date); 