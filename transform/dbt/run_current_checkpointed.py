#!/usr/bin/env python3
"""Incremental, single-writer, checkpointed application of staged CDC to current state.

Each bounded dbt batch appends new state versions. ReplacingMergeTree(kafka_offset)
with FINAL resolves to the latest logical state. A crash before the checkpoint
commits may replay the batch without changing the logical result. Source staging
checkpoints are the *only* upper bound; raw Kafka ingestion alone is insufficient.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

import clickhouse_connect

STREAMS = {
    'transactions': {
        'source': 'staging.stg_transactions_events',
        'target': 'staging.stg_transactions_current',
        'model': 'stg_transactions_current',
        'key': 'transaction_id',
    },
    'merchants': {
        'source': 'staging.stg_merchants_events',
        'target': 'staging.stg_merchants_current',
        'model': 'stg_merchants_current',
        'key': 'merchant_id',
    },
}


def fetch_one(client, sql):
    return client.query(sql).result_rows[0]


def ensure_checkpoint_table(client):
    client.command('''
        CREATE TABLE IF NOT EXISTS staging.current_state_checkpoints
        (
            stream LowCardinality(String),
            from_offset Int64,
            to_offset Int64,
            stage_event_count UInt64,
            verified_entities UInt64,
            completed_at DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = MergeTree
        ORDER BY (stream, to_offset, completed_at)
    ''')


def checkpoint(client, table, stream):
    n, highest = fetch_one(client, f"SELECT count(), max(to_offset) FROM {table} WHERE stream='{stream}'")
    return int(highest) if n else -1


def available_high_water(client, stream):
    n, highest = fetch_one(client, f"SELECT count(), max(to_offset) FROM staging.staging_checkpoints WHERE stream='{stream}'")
    if not n:
        raise RuntimeError(f"{stream}: staging checkpoint missing; run staging controller first")
    return int(highest)


def filter_window(lo, hi):
    return f'kafka_partition=0 AND kafka_offset > {lo} AND kafka_offset <= {hi}'


def validate_current_window(client, cfg, lo, hi):
    """All touched IDs must have an applied version >= the latest event in the window.

    Reading max(version) from physical state rows is intentional; this tests the
    presence of the latest version and does not depend on background merges.
    Filtering actual to the IDs in this window avoids a full-state hash join.
    """
    f = filter_window(lo, hi)
    source, target, key = cfg['source'], cfg['target'], cfg['key']
    query = f'''
        SELECT
            count() AS touched,
            countIf(a.present = 0 OR a.actual_offset < e.expected_offset) AS missing_or_stale
        FROM
        (
            SELECT {key}, max(kafka_offset) AS expected_offset
            FROM {source} FINAL
            WHERE {f}
            GROUP BY {key}
        ) AS e
        LEFT JOIN
        (
            SELECT {key}, max(kafka_offset) AS actual_offset, toUInt8(1) AS present
            FROM {target}
            WHERE {key} IN (
                SELECT {key} FROM {source} FINAL WHERE {f}
            )
            GROUP BY {key}
        ) AS a USING ({key})
    '''
    touched, missing = fetch_one(client, query)
    events = fetch_one(client, f'SELECT count() FROM {source} FINAL WHERE {f}')[0]
    if events <= 0 or touched <= 0 or missing:
        raise RuntimeError(
            f"{cfg['model']}: failed verification window ({lo},{hi}]: "
            f"events={events}, touched={touched}, missing_or_stale={missing}"
        )
    return int(events), int(touched)


def run_stream(client, name, batch_size, max_batches, drain):
    cfg = STREAMS[name]
    prev = checkpoint(client, 'staging.current_state_checkpoints', name)
    high = available_high_water(client, name)
    if prev >= high:
        print(f'{name}: up to date (current_checkpoint={prev}; staging_checkpoint={high})', flush=True)
        return
    count = 0
    while prev < high and (drain or count < max_batches):
        end = min(prev + batch_size, high)
        variables = f'{{state_from_offset: {prev}, state_to_offset: {end}}}'
        print(f'{name}: applying staged window ({prev}, {end}]', flush=True)
        subprocess.run(['dbt', 'run', '--select', cfg['model'], '--vars', variables], check=True)
        events, touched = validate_current_window(client, cfg, prev, end)
        client.command(f'''
            INSERT INTO staging.current_state_checkpoints
            (stream, from_offset, to_offset, stage_event_count, verified_entities)
            VALUES ('{name}', {prev}, {end}, {events}, {touched})
        ''')
        prev = end
        count += 1
        print(f'{name}: checkpoint={prev}, validated_events={events}, touched_entities={touched}', flush=True)
    print(f'{name}: stopped at {prev}; staging_high_water={high}', flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stream', choices=['both', *STREAMS], default='both')
    p.add_argument('--batch-size', type=int, default=100_000)
    p.add_argument('--max-batches', type=int, default=1)
    p.add_argument('--drain', action='store_true')
    args = p.parse_args()
    if not (1 <= args.batch_size <= 250_000) or args.max_batches < 1:
        p.error('--batch-size must be 1..250000 and --max-batches >= 1')
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        port=8123,
        username=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD'],
        database='default',
        connect_timeout=10,
        send_receive_timeout=1200,
    )
    ensure_checkpoint_table(client)
    requested = tuple(STREAMS) if args.stream == 'both' else (args.stream,)
    for stream in requested:
        run_stream(client, stream, args.batch_size, args.max_batches, args.drain)
    print('CURRENT_STATE_CHECKPOINT_RUN_OK', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (Exception, subprocess.CalledProcessError) as exc:
        print(f'CURRENT_STATE_CHECKPOINT_RUN_FAILED: {exc}', file=sys.stderr, flush=True)
        sys.exit(1)
