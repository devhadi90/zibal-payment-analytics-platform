{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(kafka_offset)',
    order_by='merchant_id',
    on_schema_change='fail',
    tags=['current_state', 'merchants']
  )
}}

-- One latest logical row per merchant_id with FINAL; no plaintext PII.
-- This is current merchant state, not the SCD2 history dimension.
SELECT
    merchant_id,
    kafka_offset,
    source_lsn,
    op,
    name,
    mcc,
    province,
    risk_level,
    updated_at,
    owner_national_id_masked,
    owner_mobile_masked,
    is_deleted
FROM {{ ref('stg_merchants_events') }} FINAL
WHERE kafka_partition = 0
  AND kafka_offset > {{ var('state_from_offset', -1) | int }}
  AND kafka_offset <= {{ var('state_to_offset', -1) | int }}
