-- ============================================================
-- NYC Taxi Analytics
-- Business Metrics
-- ============================================================


-- ------------------------------------------------------------
-- 1. Average fare per mile
-- ------------------------------------------------------------
-- Zero-distance trips are excluded because fare / 0
-- is undefined and would distort the metric.

SELECT
    ROUND(
        (
            SUM(fare_amount) / SUM(trip_distance)
        ),
        2
    ) AS average_fare_per_mile
FROM gold.fact_taxi_trips
WHERE trip_distance > 0;


-- ------------------------------------------------------------
-- 2. Peak travel hours
-- ------------------------------------------------------------
-- Higher trip_count means higher taxi activity.

SELECT
    pickup_hour,
    COUNT(*) AS trip_count
FROM gold.fact_taxi_trips
GROUP BY pickup_hour
ORDER BY trip_count DESC, pickup_hour;


-- ------------------------------------------------------------
-- 3. Total revenue by payment type
-- ------------------------------------------------------------
-- The fact table stores payment_type_key.
-- Human-readable labels come from the dimension table.

SELECT
    payment.payment_type_name,
    COUNT(*) AS trip_count,
    ROUND(
        SUM(fact.total_amount),
        2
    ) AS total_revenue
FROM gold.fact_taxi_trips AS fact
INNER JOIN gold.dim_payment_type AS payment
    ON fact.payment_type_key = payment.payment_type_key
GROUP BY
    payment.payment_type_name
ORDER BY
    total_revenue DESC;