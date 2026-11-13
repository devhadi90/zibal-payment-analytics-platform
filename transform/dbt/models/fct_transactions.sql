{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(kafka_offset)',
    order_by='transaction_id',
    on_schema_change='fail',
    schema='marts',
    tags=['transactions_fact', 'point_in_time']
  )
}}

-- One logical row per transaction ID when read with FINAL.
-- Fact changes are append-only versions; hard deletes stay as is_deleted=1
-- and are excluded by downstream reporting. A failed batch may be replayed.
-- Explicit bounded CDC offsets are required (bare dbt run processes nothing).
-- A touched ID uses its LATEST current state, even if that state comes from a
-- later CDC offset than the triggering window. Replays are safe.
-- The ASOF match is performed at transaction CREATED_AT, NOT updated_at.
-- History before the merchant snapshot is an assumed baseline, not known fact.

WITH touched_ids AS
(
    SELECT DISTINCT transaction_id
    FROM {{ ref('stg_transactions_events') }} FINAL
    WHERE kafka_partition = 0
      AND kafka_offset > {{ var('fact_from_offset', -1) | int }}
      AND kafka_offset <= {{ var('fact_to_offset', -1) | int }}
),
latest AS
(
    SELECT
        transaction_id,
        kafka_offset,
        source_lsn,
        merchant_id,
        channel,
        card_bin,
        amount,
        fee_amount,
        status,
        assumeNotNull(created_at) AS created_at_utc,
        updated_at,
        installments,
        is_deleted
    FROM {{ ref('stg_transactions_current') }} FINAL
    WHERE transaction_id IN (SELECT transaction_id FROM touched_ids)
      AND created_at IS NOT NULL
),
merchant_versions AS
(
    SELECT
        merchant_id,
        valid_from,
        valid_to,
        version_number,
        name,
        mcc,
        province,
        risk_level,
        is_baseline_assumption
    FROM {{ ref('dim_merchant_history') }}
    ORDER BY merchant_id, valid_from
)
SELECT
    t.transaction_id,
    t.kafka_offset,
    t.source_lsn,
    t.merchant_id,
    t.channel,
    t.card_bin,
    t.amount,
    t.fee_amount,
    t.status,
    t.created_at_utc AS created_at,
    t.updated_at,
    t.installments,
    toDate(t.created_at_utc, 'Asia/Tehran') AS created_date_tehran,
    h.version_number AS merchant_version_number,
    h.valid_from AS merchant_valid_from,
    h.valid_to AS merchant_valid_to,
    h.mcc AS mcc_at_txn,
    h.province AS province_at_txn,
    h.risk_level AS risk_level_at_txn,
    h.name AS merchant_name_at_txn,
    h.is_baseline_assumption AS merchant_baseline_assumption,
    CAST(h.version_number > 0 AND t.created_at_utc < h.valid_to AS UInt8) AS attribution_found,
    t.is_deleted AS is_deleted
FROM latest AS t
ASOF LEFT JOIN merchant_versions AS h
    ON t.merchant_id = h.merchant_id
   AND t.created_at_utc >= h.valid_from
