#!/usr/bin/env python3
"""Single-writer, crash-retry-safe staging progress for the Zibal CDC dbt pilot.

Requires the existing dbt runner image (dbt-clickhouse already installs
clickhouse-connect). Does not advance any checkpoint before dbt + coverage
validation succeed. Never executes dbt against an unbounded range.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

import clickhouse_connect

STREAMS = {
    "transactions": ("raw.cdc_transactions", "staging.stg_transactions_events", "stg_transactions_events"),
    "merchants": ("raw.cdc_merchants", "staging.stg_merchants_events", "stg_merchants_events"),
}


def scalar(client, query):
    return client.query(query).result_rows[0][0]


def row(client, query):
    return client.query(query).result_rows[0]


def ensure_checkpoints(client):
    client.command("CREATE DATABASE IF NOT EXISTS staging")
    client.command("""
        CREATE TABLE IF NOT EXISTS staging.staging_checkpoints
        (
            stream LowCardinality(String),
            from_offset Int64,
            to_offset Int64,
            raw_event_count UInt64,
            stage_event_count UInt64,
            committed_at DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = MergeTree
        ORDER BY (stream, to_offset, committed_at)
    """)


def checkpoint(client, stream):
    count, high = row(client, f"SELECT count(), max(to_offset) FROM staging.staging_checkpoints WHERE stream = '{stream}'")
    return int(high) if count else None


def max_offset(client, table):
    count, high = row(client, f"SELECT count(), max(toInt64(kafka_offset)) FROM {table} WHERE kafka_partition = 0")
    return int(high) if count else -1


def window_filter(lo, hi):
    # All checked-in CDC topics currently have precisely one partition (0).
    return f"kafka_partition = 0 AND kafka_offset > {lo} AND kafka_offset <= {hi}"


def validate_window(client, source, target, lo, hi):
    f = window_filter(lo, hi)
    src_count, src_unique = row(client, f"SELECT count(), uniqExact(tuple(kafka_topic, kafka_partition, kafka_offset)) FROM {source} FINAL WHERE {f}")
    dst_count, dst_unique = row(client, f"SELECT count(), uniqExact(tuple(kafka_topic, kafka_partition, kafka_offset)) FROM {target} FINAL WHERE {f}")
    if src_count != src_unique or dst_count != dst_unique or src_count != dst_count:
        raise RuntimeError(f"Coverage count mismatch {source}->{target} ({lo}, {hi}]: raw={src_count}/{src_unique}, staging={dst_count}/{dst_unique}")

    fields = "kafka_topic, kafka_partition, kafka_offset"
    def anti(left, right):
        return f"""
            SELECT count() FROM (
                SELECT {fields} FROM {left} FINAL WHERE {f}
            ) AS l LEFT ANTI JOIN (
                SELECT {fields} FROM {right} FINAL WHERE {f}
            ) AS r USING ({fields})
        """
    missing = scalar(client, anti(source, target))
    unexpected = scalar(client, anti(target, source))
    if missing or unexpected:
        raise RuntimeError(f"Identity mismatch {source}->{target} ({lo}, {hi}]: missing={missing} unexpected={unexpected}")
    return int(src_count), int(dst_count)


def record_checkpoint(client, stream, lo, hi, raw_count, stage_count):
    # Append-only audit: a crash before this INSERT causes a harmless replay of
    # the window on the next run. Committing here does not imply marts are current.
    client.command(f"""
        INSERT INTO staging.staging_checkpoints
        (stream, from_offset, to_offset, raw_event_count, stage_event_count)
        VALUES ('{stream}', {lo}, {hi}, {raw_count}, {stage_count})
    """)


def bootstrap_existing_pilot(client, stream, source, target):
    existing = checkpoint(client, stream)
    if existing is not None:
        return existing
    stage_hi = max_offset(client, target)
    if stage_hi < 0:
        return -1
    # The prior manual pilots must cover the raw *prefix* exactly. Do not infer
    # a checkpoint from MAX(staged.kafka_offset) without this verification.
    raw_count, stage_count = validate_window(client, source, target, -1, stage_hi)
    record_checkpoint(client, stream, -1, stage_hi, raw_count, stage_count)
    print(f"{stream}: verified existing prefix 0..{stage_hi}; checkpoint bootstrapped", flush=True)
    return stage_hi


def run_stream(client, stream, batch_size, max_batches, drain):
    source, target, model = STREAMS[stream]
    last = bootstrap_existing_pilot(client, stream, source, target)
    high_water = max_offset(client, source)  # Freeze boundary for this invocation.
    if high_water <= last:
        print(f"{stream}: up to date (checkpoint={last}, raw_high_water={high_water})", flush=True)
        return
    total = 0
    while last < high_water and (drain or total < max_batches):
        end = min(last + batch_size, high_water)
        variables = f"{{from_offset: {last}, to_offset: {end}}}"
        command = ["dbt", "run", "--select", model, "--vars", variables]
        print(f"{stream}: dbt window ({last}, {end}]", flush=True)
        subprocess.run(command, check=True)
        raw_n, stage_n = validate_window(client, source, target, last, end)
        record_checkpoint(client, stream, last, end, raw_n, stage_n)
        last = end
        total += 1
        print(f"{stream}: committed checkpoint={last}, verified_events={stage_n}", flush=True)
    print(f"{stream}: stopped at checkpoint={last}, invocation_raw_high_water={high_water}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream", choices=["both", *STREAMS], default="both")
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--max-batches", type=int, default=1, help="Per stream; default processes one bounded batch")
    parser.add_argument("--drain", action="store_true", help="Process all raw events present at invocation start")
    args = parser.parse_args()
    if args.batch_size < 1 or args.batch_size > 250_000 or args.max_batches < 1:
        parser.error("batch-size must be 1..250000 and max-batches must be positive")
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=8123,
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="default",
        connect_timeout=10,
        send_receive_timeout=1200,
    )
    ensure_checkpoints(client)
    streams = STREAMS.keys() if args.stream == "both" else (args.stream,)
    for stream in streams:
        run_stream(client, stream, args.batch_size, args.max_batches, args.drain)
    print("STAGING_CHECKPOINT_RUN_OK", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (Exception, subprocess.CalledProcessError) as exc:
        print(f"STAGING_CHECKPOINT_RUN_FAILED: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
