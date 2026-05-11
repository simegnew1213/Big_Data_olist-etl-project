-- models/staging/stg_orders.sql
-- Staging model: clean and standardize the raw orders_full table.
-- Materialized as a view (no storage cost) for fast downstream use.

{{
  config(
    materialized = 'view',
    description  = 'Cleaned and typed orders with delivery metrics'
  )
}}

SELECT
    order_id,
    customer_id,
    customer_state,
    customer_city,

    -- Timestamps
    CAST(order_purchase_timestamp AS TIMESTAMP)      AS purchase_ts,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_ts,
    CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_ts,

    -- Date keys
    CAST(purchase_date  AS DATE) AS purchase_date,
    CAST(delivery_date  AS DATE) AS delivery_date,
    CAST(estimated_date AS DATE) AS estimated_date,

    -- Numeric metrics (coalesce nulls to 0)
    COALESCE(total_payment,  0) AS total_payment,
    COALESCE(delivery_days,  0) AS delivery_days,
    COALESCE(item_count,     0) AS item_count,
    COALESCE(items_total,    0) AS items_total,
    COALESCE(freight_total,  0) AS freight_total,

    -- Flags
    was_late,
    CASE WHEN was_late = 1 THEN 'Late' ELSE 'On Time' END AS delivery_status,

    -- Calendar
    year,
    month,

    -- Weather
    COALESCE(temp_max,      NULL) AS temp_max,
    COALESCE(precipitation, 0)    AS precipitation,
    rain_category

FROM {{ source('olist', 'orders_full') }}
WHERE order_id IS NOT NULL
  AND total_payment IS NOT NULL
  AND total_payment > 0
