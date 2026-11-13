{{ config(materialized='view', schema='marts', tags=['reporting_aggregates']) }}

-- A date is visible only after all five kinds of its snapshot are inserted.
-- Reading publications FINAL resolves the latest committed revision per date.
SELECT
    r.date AS date,
    r.risk_level AS risk_level,
    r.success_count AS success_count,
    r.success_amount AS success_amount
FROM marts.daily_metric_rows AS r
INNER JOIN
(
    SELECT date, run_id
    FROM marts.daily_metric_publications FINAL
) AS p
    ON r.date = p.date AND r.run_id = p.run_id
WHERE r.kind = 'risk'
