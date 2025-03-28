-- Gold Layer: Correlations

USE macro;

-- Table for storing correlations between economic indicators
CREATE TABLE IF NOT EXISTS gold_economic_indicators_correlations
(
    indicator_id_1 UUID,
    indicator_id_2 UUID,
    correlation Float64,
    period_start Date,
    period_end Date,
    min_periods UInt32,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYear(period_start)
ORDER BY (indicator_id_1, indicator_id_2, period_start);

-- Materialized view for monthly correlations
CREATE MATERIALIZED VIEW IF NOT EXISTS macro.gold_economic_indicators_monthly_correlations
ENGINE = AggregatingMergeTree()
PARTITION BY toYear(period_start)
ORDER BY (indicator_id_1, indicator_id_2, period_start)
AS SELECT
    c.indicator_id_1,
    c.indicator_id_2,
    avg(c.correlation) as correlation,
    toStartOfMonth(c.period_start) as period_start,
    toStartOfMonth(c.period_end) as period_end,
    max(c.min_periods) as min_periods
FROM macro.gold_economic_indicators_correlations c
GROUP BY c.indicator_id_1, c.indicator_id_2, toStartOfMonth(c.period_start), toStartOfMonth(c.period_end); 