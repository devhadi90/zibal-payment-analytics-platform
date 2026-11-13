{{ config(materialized='view', schema='marts', tags=['reporting_facts', 'merchant_reattribution']) }}

-- The physical fct_transactions holds current transaction state and the
-- attribution known at the time its transaction event was last processed.
-- Merchant-only changes do not increase the transaction Kafka offset, so we
-- must NOT append a second physical fact with the same ReplacingMergeTree
-- version. Resolve temporal attributes from the tiny SCD2 dimension here.
-- All five aggregates read this canonical reporting view.
WITH merchant_versions AS
(
    SELECT merchant_id, valid_from, valid_to, version_number,
           name, mcc, province, risk_level, is_baseline_assumption
    FROM {{ ref('dim_merchant_history') }}
    ORDER BY merchant_id, valid_from
)
SELECT
    f.transaction_id,
    f.kafka_offset,
    f.source_lsn,
    f.merchant_id,
    f.channel,
    f.card_bin,
    f.amount,
    f.fee_amount,
    f.status,
    f.created_at,
    f.updated_at,
    f.installments,
    f.created_date_tehran,
    h.version_number AS merchant_version_number,
    h.valid_from AS merchant_valid_from,
    h.valid_to AS merchant_valid_to,
    h.mcc AS mcc_at_txn,
    h.province AS province_at_txn,
    h.risk_level AS risk_level_at_txn,
    h.name AS merchant_name_at_txn,
    h.is_baseline_assumption AS merchant_baseline_assumption,
    CAST(h.version_number > 0 AND f.created_at < h.valid_to AS UInt8) AS attribution_found,
    f.is_deleted
FROM {{ ref('fct_transactions') }} AS f FINAL
ASOF LEFT JOIN merchant_versions AS h
    ON f.merchant_id = h.merchant_id
   AND f.created_at >= h.valid_from
