-- Zero rows on success: the latest SCD version equals the current merchant state.
SELECT c.merchant_id
FROM
(
    SELECT merchant_id, name, mcc, province, risk_level
    FROM {{ ref('stg_merchants_current') }} FINAL
    WHERE is_deleted = 0
) AS c
LEFT JOIN
(
    SELECT merchant_id, name, mcc, province, risk_level
    FROM {{ ref('dim_merchant_history') }}
    WHERE is_current = 1
) AS h ON c.merchant_id = h.merchant_id
WHERE h.merchant_id = 0
   OR c.name != h.name
   OR c.mcc != h.mcc
   OR c.province != h.province
   OR c.risk_level != h.risk_level
