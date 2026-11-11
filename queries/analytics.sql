-- ============================================================
-- Serverless Data Pipeline - Athena Analytics Queries
-- Author: @ingmarma | MarmaTech
-- ============================================================
-- These queries run on the processed Parquet data in Athena.
-- Workgroup: serverless-data-pipeline-{env}
-- ============================================================

-- 1. Monthly transaction summary
-- Cost: ~$0.002 per query (Parquet columnar scan)
SELECT
    year,
    month,
    COUNT(*)                    AS transaction_count,
    ROUND(SUM(amount), 2)       AS total_amount,
    ROUND(AVG(amount), 2)       AS avg_amount,
    ROUND(APPROX_PERCENTILE(amount, 0.5), 2)  AS median_amount,
    ROUND(APPROX_PERCENTILE(amount, 0.95), 2) AS p95_amount
FROM transactions
GROUP BY year, month
ORDER BY year, month;


-- 2. Top categories by revenue
SELECT
    category,
    COUNT(*)                    AS tx_count,
    ROUND(SUM(amount), 2)       AS total_revenue,
    ROUND(AVG(amount), 2)       AS avg_ticket,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM transactions
WHERE status = 'completed'
GROUP BY category
ORDER BY total_revenue DESC;


-- 3. Daily trend with 7-day rolling average
WITH daily AS (
    SELECT
        DATE(transaction_date)  AS tx_date,
        COUNT(*)                AS tx_count,
        ROUND(SUM(amount), 2)   AS daily_total
    FROM transactions
    WHERE status = 'completed'
    GROUP BY DATE(transaction_date)
)
SELECT
    tx_date,
    tx_count,
    daily_total,
    ROUND(AVG(daily_total) OVER (
        ORDER BY tx_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_7d_avg
FROM daily
ORDER BY tx_date;


-- 4. Hourly heatmap (best hours for transactions)
SELECT
    hour,
    day_of_week,
    COUNT(*)              AS tx_count,
    ROUND(SUM(amount), 2) AS total_amount
FROM transactions
WHERE status = 'completed'
GROUP BY hour, day_of_week
ORDER BY day_of_week, hour;


-- 5. Country breakdown
SELECT
    country,
    currency,
    COUNT(*)                    AS tx_count,
    ROUND(SUM(amount), 2)       AS total_amount,
    ROUND(AVG(amount), 2)       AS avg_amount,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM transactions
WHERE status = 'completed'
GROUP BY country, currency
ORDER BY total_amount DESC;


-- 6. Payment method analysis
SELECT
    payment_method,
    COUNT(*)                AS tx_count,
    ROUND(SUM(amount), 2)   AS total_amount,
    ROUND(AVG(amount), 2)   AS avg_amount,
    ROUND(
        CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS DOUBLE)
        / COUNT(*) * 100, 2
    ) AS failure_rate_pct
FROM transactions
GROUP BY payment_method
ORDER BY total_amount DESC;


-- 7. Amount distribution buckets
SELECT
    amount_bucket,
    COUNT(*)              AS tx_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
WHERE status = 'completed'
GROUP BY amount_bucket
ORDER BY
    CASE amount_bucket
        WHEN 'micro' THEN 1
        WHEN 'small' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'large' THEN 4
        WHEN 'enterprise' THEN 5
    END;


-- 8. Weekend vs weekday comparison
SELECT
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS period,
    COUNT(*)              AS tx_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
WHERE status = 'completed'
GROUP BY is_weekend;


-- 9. Data quality: rejected records check
-- Run this against the rejected path if needed
-- SELECT * FROM rejected_transactions LIMIT 100;


-- 10. Cost estimation query
-- Athena charges $5 per TB scanned.
-- Parquet with Snappy compression + columnar = minimal scan.
-- Example: scanning only 'amount' + 'category' columns from 100GB raw
-- scans ~2GB of Parquet data = $0.01 per query.
