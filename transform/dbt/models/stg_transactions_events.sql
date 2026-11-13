{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(kafka_topic, kafka_partition, kafka_offset)',
    on_schema_change='fail',
    tags=['stage_transactions']
  )
}}

-- Deliberately bounded. A bare dbt run selects no raw records; the driver
-- must supply from_offset/to_offset. Replay of a range is logically idempotent
-- through ReplacingMergeTree + FINAL, not physically exactly once.
WITH events AS (
    SELECT
        kafka_topic, kafka_partition, kafka_offset,
        source_lsn, source_ts_ms, source_txid, op,
        if(op = 'd', before_json, after_json) AS row_json
    FROM {{ source('raw', 'cdc_transactions') }} FINAL
    WHERE kafka_partition = 0
      AND kafka_offset > {{ var('from_offset', -1) | int }}
      AND kafka_offset <= {{ var('to_offset', -1) | int }}
)
SELECT
    kafka_topic, kafka_partition, kafka_offset,
    source_lsn, source_ts_ms, source_txid, op,
    JSONExtractUInt(row_json, 'transaction_id') AS transaction_id,
    JSONExtractUInt(row_json, 'merchant_id') AS merchant_id,
    JSONExtractString(row_json, 'channel') AS channel,
    JSONExtractString(row_json, 'card_bin') AS card_bin,
    JSONExtractInt(row_json, 'amount') AS amount,
    JSONExtractInt(row_json, 'fee_amount') AS fee_amount,
    JSONExtractString(row_json, 'status') AS status,
    parseDateTime64BestEffortOrNull(JSONExtractString(row_json, 'created_at'), 6, 'UTC') AS created_at,
    parseDateTime64BestEffortOrNull(JSONExtractString(row_json, 'updated_at'), 6, 'UTC') AS updated_at,
    if(JSONExtractRaw(row_json, 'installments') IN ('', 'null'),
       CAST(NULL AS Nullable(Int32)),
       toInt32(JSONExtractInt(row_json, 'installments'))) AS installments,
    CAST(op = 'd' AS UInt8) AS is_deleted
FROM events
