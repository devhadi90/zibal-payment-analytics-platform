-- Append-only logical CDC event journal; one table per source table.
-- Contains original before/after JSON, INCLUDING PII. Never grant reporting users raw access.
-- ReplacingMergeTree keeps replayed Kafka offsets logically unique. Reads must use FINAL
-- (or explicitly deduplicate event identity); physical duplicates can remain until merges.
CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.cdc_merchants
(
    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset UInt64,
    kafka_ts_ms Int64,
    source_lsn Nullable(UInt64),
    source_ts_ms Nullable(Int64),
    source_txid Nullable(UInt64),
    op LowCardinality(String),
    before_json String,
    after_json String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
ORDER BY (kafka_topic, kafka_partition, kafka_offset);

CREATE TABLE IF NOT EXISTS raw.cdc_transactions
(
    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset UInt64,
    kafka_ts_ms Int64,
    source_lsn Nullable(UInt64),
    source_ts_ms Nullable(Int64),
    source_txid Nullable(UInt64),
    op LowCardinality(String),
    before_json String,
    after_json String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
ORDER BY (kafka_topic, kafka_partition, kafka_offset);
