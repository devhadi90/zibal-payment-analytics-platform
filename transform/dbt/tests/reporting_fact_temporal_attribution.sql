-- Should return zero records. This runs over all active transaction facts.
SELECT transaction_id
FROM {{ ref('fct_transactions_reporting') }}
WHERE is_deleted = 0
  AND (attribution_found = 0 OR created_at < merchant_valid_from OR created_at >= merchant_valid_to)
LIMIT 10
SETTINGS max_threads = 2
