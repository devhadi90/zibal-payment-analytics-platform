{{
  config(
    materialized='view',
    schema='marts',
    tags=['scd2', 'merchant_history']
  )
}}

-- Merchant SCD Type 2 history, computed from incrementally staged CDC events.
-- A view is intentional: the merchant stream is tiny (~2k snapshot + ~110 changes),
-- so there is no separate history checkpoint or mutation of previous versions.
-- Only changes to (name, mcc, province, risk_level) create a new version.
-- First snapshot starts at 1970: historical values before CDC are UNKNOWN and
-- represented by the snapshot baseline assumption, NOT reconstructed truth.
-- Validity is [valid_from, valid_to) in UTC; last valid_to is 2100-01-01.
-- Source updated_at is used as effective time; source offset orders CDC events.

WITH ordered_events AS
(
    SELECT
        merchant_id,
        kafka_offset,
        source_lsn,
        op,
        name,
        mcc,
        province,
        risk_level,
        if(
            op = 'r',
            toDateTime64('1970-01-01 00:00:00', 6, 'UTC'),
            assumeNotNull(updated_at)
        ) AS effective_from,
        tuple(name, mcc, province, risk_level) AS tracked_values,
        row_number() OVER
            (PARTITION BY merchant_id ORDER BY kafka_offset) AS event_number,
        lagInFrame(tuple(name, mcc, province, risk_level), 1) OVER
            (PARTITION BY merchant_id ORDER BY kafka_offset
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS previous_values
    FROM {{ ref('stg_merchants_events') }} FINAL
    WHERE kafka_partition = 0
      AND op IN ('r', 'c', 'u')
),
change_points AS
(
    SELECT
        merchant_id, kafka_offset, source_lsn, op,
        name, mcc, province, risk_level, effective_from
    FROM ordered_events
    WHERE event_number = 1 OR tracked_values != previous_values
),
ranged_versions AS
(
    SELECT
        merchant_id,
        kafka_offset AS starting_kafka_offset,
        source_lsn AS starting_source_lsn,
        op AS starting_op,
        name,
        mcc,
        province,
        risk_level,
        effective_from AS valid_from,
        leadInFrame(
            effective_from, 1,
            toDateTime64('2100-01-01 00:00:00', 6, 'UTC')
        ) OVER
            (PARTITION BY merchant_id ORDER BY kafka_offset
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS valid_to,
        row_number() OVER
            (PARTITION BY merchant_id ORDER BY kafka_offset) AS version_number
    FROM change_points
)
SELECT
    merchant_id,
    version_number,
    starting_kafka_offset,
    starting_source_lsn,
    name,
    mcc,
    province,
    risk_level,
    valid_from,
    valid_to,
    CAST(version_number = 1 AS UInt8) AS is_baseline_assumption,
    CAST(valid_to = toDateTime64('2100-01-01 00:00:00', 6, 'UTC') AS UInt8) AS is_current
FROM ranged_versions
