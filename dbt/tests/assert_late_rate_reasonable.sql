-- tests/assert_late_rate_reasonable.sql
-- Custom dbt test: late delivery rate must be between 0% and 50%.
-- A rate above 50% would indicate a data quality issue.
-- Returns 0 rows to pass.

SELECT
    customer_state,
    late_rate_pct
FROM {{ ref('revenue_by_state') }}
WHERE late_rate_pct < 0
   OR late_rate_pct > 50
