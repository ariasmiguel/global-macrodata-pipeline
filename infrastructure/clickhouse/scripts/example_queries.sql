-- Example queries for the macroeconomic data pipeline

-- 1. Basic queries with joins
-- Get all values for a specific indicator with its metadata
SELECT 
    v.date,
    v.value,
    m.indicator_name,
    m.source,
    m.industry_name,
    m.seasonal_adjustment
FROM macro.economic_indicators_values v
JOIN macro.economic_indicators_metadata m ON v.series_id = m.series_id
WHERE m.indicator_name LIKE '%PPI%'
ORDER BY v.date DESC
LIMIT 10;

-- 2. Aggregations using materialized views
-- Get monthly averages for all indicators
SELECT 
    m.indicator_name,
    m.industry_name,
    mv.date,
    mv.avg_value,
    mv.min_value,
    mv.max_value
FROM macro.economic_indicators_monthly mv
JOIN macro.economic_indicators_metadata m ON mv.series_id = m.series_id
WHERE mv.date >= '2023-01-01'
ORDER BY mv.date DESC, m.indicator_name
LIMIT 20;

-- 3. YoY and MoM changes
-- Get the latest changes for all indicators
SELECT 
    m.indicator_name,
    m.industry_name,
    c.date,
    c.value,
    round(c.mom_change * 100, 2) as mom_change_pct,
    round(c.yoy_change * 100, 2) as yoy_change_pct
FROM macro.economic_indicators_changes c
JOIN macro.economic_indicators_metadata m ON c.series_id = m.series_id
WHERE c.date = (
    SELECT max(date) 
    FROM macro.economic_indicators_changes
)
ORDER BY abs(c.yoy_change_pct) DESC
LIMIT 10;

-- 4. Time series analysis
-- Get the latest 12 months of data for a specific indicator
WITH latest_date AS (
    SELECT max(date) as max_date
    FROM macro.economic_indicators_values
)
SELECT 
    v.date,
    v.value,
    m.indicator_name,
    m.industry_name,
    round((v.value / first_value(v.value) OVER (ORDER BY v.date) - 1) * 100, 2) as change_from_start_pct
FROM macro.economic_indicators_values v
JOIN macro.economic_indicators_metadata m ON v.series_id = m.series_id
CROSS JOIN latest_date
WHERE v.date >= addMonths(latest_date.max_date, -12)
    AND m.indicator_name LIKE '%PPI%'
ORDER BY v.date;

-- 5. Cross-indicator analysis
-- Compare YoY changes between different indicators
WITH latest_changes AS (
    SELECT 
        c.series_id,
        c.date,
        c.yoy_change
    FROM macro.economic_indicators_changes c
    WHERE c.date = (
        SELECT max(date) 
        FROM macro.economic_indicators_changes
    )
)
SELECT 
    m1.indicator_name as indicator1,
    m2.indicator_name as indicator2,
    round(lc1.yoy_change * 100, 2) as indicator1_yoy_change,
    round(lc2.yoy_change * 100, 2) as indicator2_yoy_change,
    round((lc1.yoy_change - lc2.yoy_change) * 100, 2) as change_difference
FROM latest_changes lc1
JOIN latest_changes lc2 ON lc1.date = lc2.date AND lc1.series_id < lc2.series_id
JOIN macro.economic_indicators_metadata m1 ON lc1.series_id = m1.series_id
JOIN macro.economic_indicators_metadata m2 ON lc2.series_id = m2.series_id
ORDER BY abs(lc1.yoy_change - lc2.yoy_change) DESC
LIMIT 10;

-- 6. Data quality checks
-- Check for missing values in the last 3 months
WITH latest_date AS (
    SELECT max(date) as max_date
    FROM macro.economic_indicators_values
)
SELECT 
    m.indicator_name,
    m.industry_name,
    count(*) as total_values,
    countIf(value IS NULL) as null_values,
    round(countIf(value IS NULL) / count(*) * 100, 2) as null_percentage
FROM macro.economic_indicators_values v
JOIN macro.economic_indicators_metadata m ON v.series_id = m.series_id
CROSS JOIN latest_date
WHERE v.date >= addMonths(latest_date.max_date, -3)
GROUP BY m.indicator_name, m.industry_name
HAVING null_percentage > 0
ORDER BY null_percentage DESC; 