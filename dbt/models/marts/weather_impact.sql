-- models/marts/weather_impact.sql
-- Mart model: how precipitation categories affect order volume and value.

{{
  config(
    materialized = 'table',
    description  = 'Order volume and value split by weather (rain) category'
  )
}}

SELECT
    rain_category,
    COUNT(order_id)                     AS total_orders,
    ROUND(AVG(total_payment), 2)        AS avg_order_value,
    ROUND(SUM(total_payment), 2)        AS total_revenue,
    ROUND(AVG(precipitation), 2)        AS avg_precipitation_mm,
    ROUND(AVG(temp_max), 1)             AS avg_temp_max_c
FROM {{ ref('stg_orders') }}
WHERE rain_category IS NOT NULL
GROUP BY rain_category
ORDER BY
    CASE rain_category
        WHEN 'No Rain'    THEN 1
        WHEN 'Light Rain' THEN 2
        WHEN 'Heavy Rain' THEN 3
        ELSE 4
    END
