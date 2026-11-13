{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(kafka_topic, kafka_partition, kafka_offset)',
    on_schema_change='fail',
    tags=['stage_merchants']
  )
}}

-- Only masked PII leaves restricted raw. This avoids embedding a hashing key
-- in ClickHouse queries (24.8 predates native HMAC); the assignment allows masking.
WITH events AS (
    SELECT
        kafka_topic, kafka_partition, kafka_offset,
        source_lsn, source_ts_ms, source_txid, op,
        if(op = 'd', before_json, after_json) AS row_json
    FROM {{ source('raw', 'cdc_merchants') }} FINAL
    WHERE kafka_partition = 0
      AND kafka_offset > {{ var('from_offset', -1) | int }}
      AND kafka_offset <= {{ var('to_offset', -1) | int }}
)
SELECT
    kafka_topic, kafka_partition, kafka_offset,
    source_lsn, source_ts_ms, source_txid, op,
    JSONExtractUInt(row_json, 'merchant_id') AS merchant_id,
    JSONExtractString(row_json, 'name') AS name,
    JSONExtractString(row_json, 'mcc') AS mcc,
    JSONExtractString(row_json, 'province') AS province,
    JSONExtractString(row_json, 'risk_level') AS risk_level,
    parseDateTime64BestEffortOrNull(JSONExtractString(row_json, 'updated_at'), 6, 'UTC') AS updated_at,
    concat('******', substring(JSONExtractString(row_json, 'owner_national_id'), -4)) AS owner_national_id_masked,
    concat('*******', substring(JSONExtractString(row_json, 'owner_mobile'), -4)) AS owner_mobile_masked,
    CAST(op = 'd' AS UInt8) AS is_deleted
FROM events
