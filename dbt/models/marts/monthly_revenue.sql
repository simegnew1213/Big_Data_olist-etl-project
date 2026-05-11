-- models/marts/monthly_revenue.sql
-- Mart model: monthly aggregated revenue and order metrics.
-- Materialized as a TABLE for fast dashboard queries.

{{
  config(
    materialized = 'table',
    description  = 'Monthly revenue, order volume, and delivery KPIs'
  )
}}

SELECT
    year,
    month,
    -- Human-readable period label
    CAST(year AS VARCHAR) || '-' || LPAD(CAST(month AS VARCHAR), 2, '0') AS period,

    -- Volume
    COUNT(order_id)                                     AS total_orders,

    -- Revenue
    ROUND(SUM(total_payment), 2)                        AS total_revenue,
    ROUND(AVG(total_payment), 2)                        AS avg_order_value,
    ROUND(MEDIAN(total_payment), 2)                     AS median_order_value,

    -- Delivery
    ROUND(AVG(delivery_days), 1)                        AS avg_delivery_days,
    SUM(was_late)                                       AS late_deliveries,
    ROUND(SUM(was_late) * 100.0 / COUNT(order_id), 2)  AS late_rate_pct,

    -- Basket
    ROUND(AVG(item_count), 1)                           AS avg_items_per_order

FROM {{ ref('stg_orders') }}
GROUP BY year, month
ORDER BY year, month
