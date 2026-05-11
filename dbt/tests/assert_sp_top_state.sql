-- tests/assert_sp_top_state.sql
-- Custom dbt test: São Paulo (SP) must be the #1 state by revenue.
-- Returns 0 rows to pass.

SELECT customer_state, revenue_rank
FROM {{ ref('revenue_by_state') }}
WHERE customer_state = 'SP'
  AND revenue_rank != 1
