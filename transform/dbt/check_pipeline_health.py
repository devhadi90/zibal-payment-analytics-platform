#!/usr/bin/env python3
"""M5-C: fail-fast ClickHouse pipeline checkpoint/CDC-connector health probe.

This is a *bounded operational signal*, NOT proof of a consistent PostgreSQL
snapshot or Kafka-to-ClickHouse delivery completeness. The M5-B strict
reconciliation needs quiescence and a verified CDC capture boundary.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import sys
from urllib.request import urlopen

import clickhouse_connect


STREAMS = {
    'transactions': ('raw.cdc_transactions', 'transactions'),
    'merchants': ('raw.cdc_merchants', 'merchants'),
}


def checkpoint(client, table: str, stream: str | None, col: str = 'to_offset') -> int:
    where = f" WHERE stream = '{stream}'" if stream is not None else ''
    count, high = client.query(
        f'SELECT count(), max({col}) FROM {table}{where}'
    ).result_rows[0]
    return int(high) if count else -1


def raw_high(client, table: str) -> int:
    count, high = client.query(
        f'SELECT count(), max(toInt64(kafka_offset)) FROM {table} WHERE kafka_partition = 0'
    ).result_rows[0]
    return int(high) if count else -1


def observed_ingestion_delay(client, table: str):
    """Delay of latest *observed* non-snapshot CDC event; not live source lag."""
    rows = client.query(f'''
        SELECT source_ts_ms, ingested_at
        FROM {table}
        WHERE source_ts_ms IS NOT NULL AND op IN ('c','u','d')
          AND kafka_partition = 0
        ORDER BY kafka_offset DESC
        LIMIT 1
    ''').result_rows
    if not rows:
        return None
    ts_ms, ingested = rows[0]
    if ingested.tzinfo is None:
        ingested = ingested.replace(tzinfo=timezone.utc)
    source = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
    return round(max(0.0, (ingested - source).total_seconds()), 3)


def check_chain(progress: dict[str, int], require_caught_up: bool, max_backlog: int):
    problems = []
    names = list(progress)
    for upstream, downstream in zip(names, names[1:]):
        if progress[downstream] > progress[upstream]:
            problems.append(f'{downstream}={progress[downstream]} ahead of {upstream}={progress[upstream]}')
    backlog = max(0, progress[names[0]] - progress[names[-1]])
    if require_caught_up and backlog:
        problems.append(f'nonzero raw-to-publication backlog={backlog}')
    elif backlog > max_backlog:
        problems.append(f'raw-to-publication backlog={backlog} exceeds {max_backlog}')
    return backlog, problems


def connector_states(url: str):
    # Connect REST tells us worker/task status, not source/WAL or consumer offsets.
    with urlopen(url.rstrip('/') + '/connectors', timeout=8) as response:
        names = json.load(response)
    if not names:
        raise RuntimeError('Debezium Connect has no registered connectors')
    found = {}
    for name in names:
        with urlopen(url.rstrip('/') + '/connectors/' + name + '/status', timeout=8) as response:
            result = json.load(response)
        state = result.get('connector', {}).get('state')
        tasks = [task.get('state') for task in result.get('tasks', [])]
        found[name] = {'connector': state, 'tasks': tasks}
    return found


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--max-backlog', type=int, default=10000,
                   help='alert if raw-to-published offset difference exceeds this (default: 10000)')
    p.add_argument('--max-observed-ingest-seconds', type=int, default=300,
                   help='alert if latest observed non-snapshot event took > N seconds to ingest')
    p.add_argument('--require-caught-up', action='store_true',
                   help='require zero raw-to-aggregate offset backlog; for quiescent runs only')
    p.add_argument('--skip-connect', action='store_true',
                   help='diagnostics only: omit Connect REST status (not for acceptance)')
    args = p.parse_args()
    if args.max_backlog < 0 or args.max_observed_ingest_seconds < 0:
        p.error('thresholds must be non-negative')
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'), port=8123,
        username=os.environ['CLICKHOUSE_USER'], password=os.environ['CLICKHOUSE_PASSWORD'],
        database='default', connect_timeout=10, send_receive_timeout=120,
    )
    problems = []
    report = {'check_time_utc': datetime.now(timezone.utc).isoformat(), 'streams': {}}
    for stream, (raw_table, stream_name) in STREAMS.items():
        chain = {
            'raw': raw_high(client, raw_table),
            'staging': checkpoint(client, 'staging.staging_checkpoints', stream_name),
            'current': checkpoint(client, 'staging.current_state_checkpoints', stream_name),
        }
        if stream == 'transactions':
            chain['fact'] = checkpoint(client, 'marts.fact_checkpoints', stream_name)
        chain['published'] = checkpoint(client, 'marts.daily_aggregate_checkpoints',
                                         None, f'to_{"transaction" if stream == "transactions" else "merchant"}_offset')
        backlog, errs = check_chain(chain, args.require_caught_up, args.max_backlog)
        delay = observed_ingestion_delay(client, raw_table)
        if delay is not None and delay > args.max_observed_ingest_seconds:
            errs.append(f'last observed CDC event ingest delay={delay}s exceeds {args.max_observed_ingest_seconds}s')
        report['streams'][stream] = {'offsets': chain, 'backlog_events': backlog,
                                      'last_observed_event_ingest_delay_s': delay}
        problems.extend(f'{stream}: {problem}' for problem in errs)
        print(f'HEALTH {stream}: offsets={chain}, backlog={backlog}, last_observed_ingest_delay_s={delay}', flush=True)
    if not args.skip_connect:
        report['connectors'] = connector_states(os.getenv('CONNECT_URL', 'http://connect:8083'))
        for name, status in report['connectors'].items():
            print(f'CONNECT {name}: {status}', flush=True)
            if status['connector'] != 'RUNNING' or not status['tasks'] or any(t != 'RUNNING' for t in status['tasks']):
                problems.append(f'Connect {name}: non-running connector or tasks')
    if problems:
        for err in problems:
            print(f'HEALTH_FAIL: {err}', file=sys.stderr, flush=True)
        raise RuntimeError(f'M5C_HEALTH_FAILED errors={len(problems)}')
    print('M5C_HEALTH_OK', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'M5C_FAILED: {exc}', file=sys.stderr, flush=True)
        sys.exit(1)
