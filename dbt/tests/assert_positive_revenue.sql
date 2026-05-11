-- tests/assert_positive_revenue.sql
-- Custom dbt test: every month must have positive revenue.
-- This query should return 0 rows to pass.

SELECT
    period,
    total_revenue
FROM {{ ref('monthly_revenue') }}
WHERE total_revenue <= 0
