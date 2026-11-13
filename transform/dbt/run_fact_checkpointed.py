#!/usr/bin/env python3
"""Apply bounded transaction CDC changes to marts.fct_transactions.

Single-writer, at-least-once fact publishing with independent durable checkpoints.
Do NOT advance if upstream current-state/merchant checkpoints are stale.
A failure before checkpoint INSERT replays the same bounded interval safely.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

import clickhouse_connect


def scalar(client, sql):
    return client.query(sql).result_rows[0][0]


def checkpoint(client, table, stream):
    row = client.query(
        f"SELECT count(), max(to_offset) FROM {table} WHERE stream='{stream}'"
    ).result_rows[0]
    return int(row[1]) if row[0] else -1


def ensure_checkpoint(client):
    client.command('''
        CREATE TABLE IF NOT EXISTS marts.fact_checkpoints
        (
            stream LowCardinality(String),
            from_offset Int64,
            to_offset Int64,
            touched_entities UInt64,
            completed_at DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = MergeTree
        ORDER BY (stream, to_offset, completed_at)
    ''')


def validate_window(client, lo, hi):
    # Compare latest current state and the resulting fact, for touched IDs only.
    # Equality on current kafka_offset and status detects stale replay/publication.
    f = f'kafka_partition=0 AND kafka_offset > {lo} AND kafka_offset <= {hi}'
    touched = f'''SELECT DISTINCT transaction_id
                 FROM staging.stg_transactions_events FINAL WHERE {f}'''
    sql = f'''
        SELECT
            count() AS total,
            countIf(
                f.transaction_id = 0
                OR f.kafka_offset != s.kafka_offset
                OR f.is_deleted != s.is_deleted
                OR f.status != s.status
                OR f.merchant_id != s.merchant_id
                OR f.attribution_found != 1
            ) AS mismatches
        FROM
        (
            SELECT transaction_id, kafka_offset, is_deleted, status, merchant_id
            FROM staging.stg_transactions_current FINAL
            WHERE transaction_id IN ({touched})
        ) AS s
        LEFT JOIN
        (
            SELECT transaction_id, kafka_offset, is_deleted, status,
                   merchant_id, attribution_found
            FROM marts.fct_transactions FINAL
            WHERE transaction_id IN ({touched})
        ) AS f USING (transaction_id)
    '''
    total, mismatches = client.query(sql).result_rows[0]
    expected = int(scalar(client, f'SELECT uniqExact(transaction_id) FROM staging.stg_transactions_events FINAL WHERE {f}'))
    if total != expected or mismatches:
        raise RuntimeError(
            f'fact verification failed for ({lo}, {hi}]: touched={expected}, '
            f'current={total}, mismatches={mismatches}'
        )
    return int(total)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch-size', type=int, default=100_000)
    parser.add_argument('--max-batches', type=int, default=1)
    parser.add_argument('--drain', action='store_true')
    args = parser.parse_args()
    if not (1 <= args.batch_size <= 250_000) or args.max_batches < 1:
        parser.error('--batch-size must be 1..250000 and --max-batches >= 1')

    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        port=8123,
        username=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD'],
        database='default',
        connect_timeout=10,
        send_receive_timeout=1200,
    )
    ensure_checkpoint(client)
    # We intentionally take the CURRENT-STATE checkpoint as input, never Kafka
    # committed offsets or raw ingestion high water.
    high = checkpoint(client, 'staging.current_state_checkpoints', 'transactions')
    if high < 0:
        raise RuntimeError('current-state transactions checkpoint missing')
    merchant_current = checkpoint(client, 'staging.current_state_checkpoints', 'merchants')
    merchant_staged = checkpoint(client, 'staging.staging_checkpoints', 'merchants')
    if merchant_current < merchant_staged:
        raise RuntimeError(
            f'merchant state is behind staged history: current={merchant_current}, staged={merchant_staged}'
        )
    previous = checkpoint(client, 'marts.fact_checkpoints', 'transactions')
    if previous > high:
        raise RuntimeError(f'fact checkpoint {previous} exceeds current-state checkpoint {high}')
    count = 0
    while previous < high and (args.drain or count < args.max_batches):
        end = min(previous + args.batch_size, high)
        variables = f'{{fact_from_offset: {previous}, fact_to_offset: {end}}}'
        print(f'facts: applying source window ({previous}, {end}]', flush=True)
        subprocess.run(['dbt', 'run', '--select', 'fct_transactions', '--vars', variables], check=True)
        touched = validate_window(client, previous, end)
        client.command(f'''
            INSERT INTO marts.fact_checkpoints
            (stream, from_offset, to_offset, touched_entities)
            VALUES ('transactions', {previous}, {end}, {touched})
        ''')
        previous = end
        count += 1
        print(f'facts: committed checkpoint={previous}, validated_entities={touched}', flush=True)
    if previous == high:
        print(f'facts: up to date (checkpoint={previous}; current_state_high_water={high})', flush=True)
    else:
        print(f'facts: stopped at checkpoint={previous}; current_state_high_water={high}', flush=True)
    print('FACT_CHECKPOINT_RUN_OK', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (Exception, subprocess.CalledProcessError) as exc:
        print(f'FACT_CHECKPOINT_RUN_FAILED: {exc}', file=sys.stderr, flush=True)
        sys.exit(1)
