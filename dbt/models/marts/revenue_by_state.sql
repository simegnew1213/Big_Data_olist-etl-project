-- models/marts/revenue_by_state.sql
-- Mart model: geographic revenue breakdown by Brazilian state.

{{
  config(
    materialized = 'table',
    description  = 'Revenue and delivery performance aggregated by customer state'
  )
}}

SELECT
    customer_state,

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

    -- Revenue rank (1 = highest revenue state)
    RANK() OVER (ORDER BY SUM(total_payment) DESC)      AS revenue_rank

FROM {{ ref('stg_orders') }}
WHERE customer_state IS NOT NULL
GROUP BY customer_state
ORDER BY total_revenue DESC
