-- Zero rows on success: consecutive ranges have a shared boundary.
WITH history AS
(
    SELECT
        merchant_id,
        version_number,
        valid_from,
        lagInFrame(valid_to, 1) OVER
            (PARTITION BY merchant_id ORDER BY version_number
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS previous_valid_to
    FROM {{ ref('dim_merchant_history') }}
)
SELECT merchant_id, version_number
FROM history
WHERE version_number > 1
  AND valid_from != previous_valid_to
