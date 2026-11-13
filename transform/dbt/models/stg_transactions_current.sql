{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(kafka_offset)',
    order_by='transaction_id',
    on_schema_change='fail',
    tags=['current_state', 'transactions']
  )
}}

-- One latest logical row per transaction_id when queried with FINAL.
-- Delete events are retained as tombstones (is_deleted=1); reporting excludes them.
-- A batch may be retried after a crash; the same kafka_offset/version wins.
-- The caller MUST supply an explicit bounded offset window, even on initial create.
SELECT
    transaction_id,
    kafka_offset,
    source_lsn,
    op,
    merchant_id,
    channel,
    card_bin,
    amount,
    fee_amount,
    status,
    created_at,
    updated_at,
    installments,
    is_deleted
FROM {{ ref('stg_transactions_events') }} FINAL
WHERE kafka_partition = 0
  AND kafka_offset > {{ var('state_from_offset', -1) | int }}
  AND kafka_offset <= {{ var('state_to_offset', -1) | int }}
