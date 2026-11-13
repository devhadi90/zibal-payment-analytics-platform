-- Zero rows on success: each active merchant has one open range and all ranges positive.
SELECT merchant_id
FROM {{ ref('dim_merchant_history') }}
GROUP BY merchant_id
HAVING countIf(is_current = 1) != 1
    OR countIf(valid_from >= valid_to) > 0
    OR countIf(is_baseline_assumption = 1) != 1
