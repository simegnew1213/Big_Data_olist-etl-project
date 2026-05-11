-- models/staging/stg_weather.sql
-- Staging model: standardize weather data joined onto orders.

{{
  config(
    materialized = 'view',
    description  = 'Standardized daily weather observations for São Paulo'
  )
}}

SELECT DISTINCT
    CAST(purchase_date AS DATE) AS weather_date,
    temp_max,
    precipitation,
    rain_category,
    CASE
        WHEN temp_max >= 30 THEN 'Hot'
        WHEN temp_max >= 22 THEN 'Warm'
        ELSE 'Cool'
    END AS temp_category
FROM {{ source('olist', 'orders_full') }}
WHERE purchase_date IS NOT NULL
