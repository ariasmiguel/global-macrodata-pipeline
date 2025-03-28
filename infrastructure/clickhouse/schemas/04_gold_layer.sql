-- Gold Layer: Analytics-ready data

-- Gold layer tables for analytics-ready data
USE macro;

-- Economic indicators metadata
CREATE TABLE IF NOT EXISTS gold_economic_indicators_metadata
(
    indicator_id UUID DEFAULT generateUUIDv4(),
    series_id UUID,
    raw_series_id String,
    indicator_name String,
    indicator_type String,
    description String,
    frequency String,
    units String,
    seasonal_adjustment String,
    category String,
    subcategory String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (indicator_id);

-- Economic indicators values
CREATE TABLE IF NOT EXISTS gold_economic_indicators_values
(
    indicator_id UUID,
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
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (indicator_id, date);

-- Materialized view for monthly aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS macro.gold_economic_indicators_monthly
ENGINE = AggregatingMergeTree()
PARTITION BY toYear(month_date)
ORDER BY (series_id, month_date)
AS SELECT
    v.series_id,
    m.indicator_name,
    m.category,
    m.subcategory,
    toStartOfMonth(v.date) as month_date,
    avg(v.value) as avg_value,
    min(v.value) as min_value,
    max(v.value) as max_value
FROM macro.gold_economic_indicators_values v
JOIN macro.gold_economic_indicators_metadata m ON v.series_id = m.series_id
WHERE v.is_annual_avg = 0
GROUP BY v.series_id, m.indicator_name, m.category, m.subcategory, toStartOfMonth(v.date);

-- Materialized view for MoM and YoY changes
CREATE MATERIALIZED VIEW IF NOT EXISTS macro.gold_economic_indicators_changes
ENGINE = AggregatingMergeTree()
PARTITION BY toYear(date)
ORDER BY (series_id, date)
AS SELECT
    v.series_id,
    m.indicator_name,
    m.category,
    m.subcategory,
    v.date,
    v.value,
    v.value / lagInFrame(v.value) OVER (PARTITION BY v.series_id ORDER BY v.date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) - 1 as mom_change,
    v.value / lagInFrame(v.value) OVER (PARTITION BY v.series_id ORDER BY v.date ROWS BETWEEN 12 PRECEDING AND 12 PRECEDING) - 1 as yoy_change
FROM macro.gold_economic_indicators_values v
JOIN macro.gold_economic_indicators_metadata m ON v.series_id = m.series_id
WHERE v.is_annual_avg = 0;

-- Materialized view for yearly aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS macro.gold_economic_indicators_yearly
ENGINE = AggregatingMergeTree()
PARTITION BY year
ORDER BY (series_id, year)
AS SELECT
    v.series_id,
    m.indicator_name,
    m.category,
    m.subcategory,
    toYear(v.date) as year,
    avg(v.value) as avg_value,
    min(v.value) as min_value,
    max(v.value) as max_value,
    avg(v.value) as annual_average
FROM macro.gold_economic_indicators_values v
JOIN macro.gold_economic_indicators_metadata m ON v.series_id = m.series_id
WHERE v.is_annual_avg = 0
GROUP BY v.series_id, m.indicator_name, m.category, m.subcategory, toYear(v.date); 