#!/usr/bin/env python3
"""Publish five daily ClickHouse rollups from canonical, checkpointed transaction facts.

Minimal single-writer, append-then-publish design:
- Reads independent fact and merchant checkpoints, never Kafka offsets.
- New transaction events dirty all historical dates of touched IDs from the
  staged event log, so changed created_at and hard deletes correct old days.
- A run UUID tags all five daily metric types. A date's publication pointer is
  written only after all five INSERTs and source-total validation succeed.
- Crashes before publication leave invisible/orphaned rows, safe to retry.
- Merchant history changes trigger recalculation of affected reporting dates.
  Historical attributes are resolved from SCD2 via a canonical fact view.
- --limit-days 1 is an initial non-checkpointing pilot. Full runs commit progress.

This is not an asynchronous scheduler. Do not run two instances concurrently.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import uuid

import clickhouse_connect

METRIC_KINDS = ('sales', 'channel', 'merchant', 'fee', 'risk')


def one(client, sql):
    return client.query(sql).result_rows[0][0]


def checkpoint(client, table, stream):
    count, latest = client.query(
        f"SELECT count(), max(to_offset) FROM {table} WHERE stream = '{stream}'"
    ).result_rows[0]
    return int(latest) if int(count) else -1


def ensure_schema(client):
    client.command('CREATE DATABASE IF NOT EXISTS marts')
    client.command('''
        CREATE TABLE IF NOT EXISTS marts.daily_metric_rows
        (
            date Date,
            run_id UUID,
            kind LowCardinality(String),
            channel String,
            merchant_id UInt64,
            mcc String,
            province String,
            risk_level String,
            success_count UInt64,
            terminal_count UInt64,
            success_amount Int64,
            fee_amount Int64
        ) ENGINE = MergeTree
        ORDER BY (date, kind, run_id, merchant_id, channel, mcc, province, risk_level)
    ''')
    client.command('''
        CREATE TABLE IF NOT EXISTS marts.daily_metric_publications
        (
            date Date,
            run_id UUID,
            revision UInt64,
            transaction_offset Int64,
            merchant_offset Int64,
            published_at DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(revision)
        ORDER BY date
    ''')
    client.command('''
        CREATE TABLE IF NOT EXISTS marts.daily_aggregate_checkpoints
        (
            run_id UUID,
            from_transaction_offset Int64,
            to_transaction_offset Int64,
            from_merchant_offset Int64,
            to_merchant_offset Int64,
            dirty_date_count UInt64,
            completed_at DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = MergeTree
        ORDER BY (completed_at, to_transaction_offset, run_id)
    ''')


def latest_aggregate_progress(client):
    rows = client.query('''
        SELECT to_transaction_offset, to_merchant_offset
        FROM marts.daily_aggregate_checkpoints
        ORDER BY completed_at DESC, to_transaction_offset DESC
        LIMIT 1
    ''').result_rows
    return (int(rows[0][0]), int(rows[0][1])) if rows else (-1, -1)


def dates_to_publish(client, prev_tx, high_tx, prev_merch, high_merch):
    dates = set()
    if prev_tx < 0:
        q = 'SELECT DISTINCT created_date_tehran FROM marts.fct_transactions FINAL'
        dates.update(r[0] for r in client.query(q).result_rows)
    elif high_tx > prev_tx:
        # The entire staged event log is required: background merges of the
        # physical fact can discard old versions of changed transaction IDs.
        q = f'''
            SELECT DISTINCT toDate(assumeNotNull(created_at), 'Asia/Tehran') AS date
            FROM staging.stg_transactions_events FINAL
            WHERE created_at IS NOT NULL
              AND transaction_id IN
              (
                  SELECT DISTINCT transaction_id
                  FROM staging.stg_transactions_events FINAL
                  WHERE kafka_partition = 0
                    AND kafka_offset > {prev_tx}
                    AND kafka_offset <= {high_tx}
              )
        '''
        dates.update(r[0] for r in client.query(q).result_rows)

    if high_merch > prev_merch and prev_merch >= 0:
        # A merchant's effective_from is updated_at in SCD2. A changed version
        # can only affect transactions created at or after that effective time.
        # Compute a safe superset of dirty dates for affected merchants, without
        # rebuilding the 183-day reporting history on every change.
        changes = f'''
            FROM staging.stg_merchants_events FINAL
            WHERE kafka_partition = 0
              AND kafka_offset > {prev_merch}
              AND kafka_offset <= {high_merch}
        '''
        unsupported = client.query(f'''
            SELECT countIf(op = 'd'), countIf(updated_at IS NULL)
            {changes}
        ''').result_rows[0]
        if any(int(v) for v in unsupported):
            raise RuntimeError('Merchant delete/missing effective timestamp: unsupported for historical attribution')
        q = f'''
            SELECT DISTINCT f.created_date_tehran
            FROM marts.fct_transactions AS f FINAL
            INNER JOIN
            (
                SELECT merchant_id, min(assumeNotNull(updated_at)) AS changed_from
                {changes}
                AND op IN ('c', 'u')
                GROUP BY merchant_id
            ) AS changed
                ON f.merchant_id = changed.merchant_id
            WHERE f.created_at >= changed.changed_from
        '''
        dates.update(r[0] for r in client.query(q).result_rows)
    return sorted(dates)


def insert_metric_rows(client, dates, run_id):
    if not dates:
        return
    filt = ', '.join(f"toDate('{d.isoformat()}')" for d in dates)
    # A single completed batch publishes all dates only after all five
    # aggregates are built. Readers never query this physical table directly.
    base = f'''
        FROM marts.fct_transactions_reporting
        WHERE is_deleted = 0 AND created_date_tehran IN ({filt})
    '''
    # Values follow the physical table's explicit INSERT column order.
    prefix = '''INSERT INTO marts.daily_metric_rows
        (date, run_id, kind, channel, merchant_id, mcc, province,
         risk_level, success_count, terminal_count, success_amount, fee_amount)
    '''
    tag = f"toUUID('{run_id}')"
    zero = 'toInt64(0)'
    # Card 1: current SUCCESS only.
    client.command(prefix + f'''
        SELECT created_date_tehran, {tag}, 'sales', '', toUInt64(0), '', '', '',
               count(), toUInt64(0), sum(amount), {zero}
        {base} AND status = 'SUCCESS'
        GROUP BY created_date_tehran
        SETTINGS max_threads = 2
    ''')
    # Card 2: SUCCESS / terminal, pending excluded from denominator.
    client.command(prefix + f'''
        SELECT created_date_tehran, {tag}, 'channel', channel, toUInt64(0), '', '', '',
               countIf(status = 'SUCCESS'),
               countIf(status IN ('SUCCESS', 'FAILED', 'REVERSED')),
               {zero}, {zero}
        {base}
        GROUP BY created_date_tehran, channel
        SETTINGS max_threads = 2
    ''')
    # Card 3: current-month top merchants is a sum of daily SUCCESS amounts.
    client.command(prefix + f'''
        SELECT created_date_tehran, {tag}, 'merchant', '', merchant_id, '', '', '',
               count(), toUInt64(0), sum(amount), {zero}
        {base} AND status = 'SUCCESS'
        GROUP BY created_date_tehran, merchant_id
        SETTINGS max_threads = 2
    ''')
    # Card 4: MCC/province at transaction time.
    client.command(prefix + f'''
        SELECT created_date_tehran, {tag}, 'fee', '', toUInt64(0),
               mcc_at_txn, province_at_txn, '',
               count(), toUInt64(0), {zero}, sum(fee_amount)
        {base} AND status = 'SUCCESS'
        GROUP BY created_date_tehran, mcc_at_txn, province_at_txn
        SETTINGS max_threads = 2
    ''')
    # Card 5: daily history-aware risk, rolled up into calendar months in card.
    client.command(prefix + f'''
        SELECT created_date_tehran, {tag}, 'risk', '', toUInt64(0),
               '', '', risk_level_at_txn,
               count(), toUInt64(0), sum(amount), {zero}
        {base} AND status = 'SUCCESS'
        GROUP BY created_date_tehran, risk_level_at_txn
        SETTINGS max_threads = 2
    ''')


def validate_unpublished(client, dates, run_id):
    filt = ', '.join(f"toDate('{d.isoformat()}')" for d in dates)
    expected = client.query(f'''
        SELECT countIf(status='SUCCESS') AS succ_count,
               sumIf(amount, status='SUCCESS') AS succ_amount,
               sumIf(fee_amount, status='SUCCESS') AS succ_fee,
               countIf(status IN ('SUCCESS','FAILED','REVERSED')) AS terminal_count
        FROM marts.fct_transactions_reporting
        WHERE is_deleted = 0 AND created_date_tehran IN ({filt})
        SETTINGS max_threads=2
    ''').result_rows[0]
    actual = client.query(f'''
        SELECT kind, sum(success_count), sum(terminal_count),
               sum(success_amount), sum(fee_amount)
        FROM marts.daily_metric_rows
        WHERE run_id = toUUID('{run_id}')
        GROUP BY kind
    ''').result_rows
    observed = {str(kind): (int(n), int(term), int(amount), int(fee))
                for kind, n, term, amount, fee in actual}
    count, amount, fee, terminal = (int(v) for v in expected)
    validations = {
        'sales': (count, 0, amount, 0),
        'channel': (count, terminal, 0, 0),
        'merchant': (count, 0, amount, 0),
        'fee': (count, 0, 0, fee),
        'risk': (count, 0, amount, 0),
    }
    for kind, exp in validations.items():
        got = observed.get(kind, (0, 0, 0, 0))
        if got != exp:
            raise RuntimeError(f'{kind} mismatch before publish: expected={exp}; actual={got}')
    print(f'validated unpublished metrics: successful={count}, amount={amount}, '
          f'fee={fee}, terminal={terminal}', flush=True)


def publish(client, dates, run_id, high_tx, high_merch):
    # One publication record per day. Each points only to completed snapshots.
    # Revision monotonically increases under the documented single-writer rule.
    current_rev = one(client, 'SELECT maxOrNull(revision) FROM marts.daily_metric_publications')
    revision = (int(current_rev) if current_rev is not None else 0) + 1
    for date in dates:
        client.command(f'''
            INSERT INTO marts.daily_metric_publications
                (date, run_id, revision, transaction_offset, merchant_offset)
            SELECT toDate('{date.isoformat()}'), toUUID('{run_id}'),
                   toUInt64({revision}), toInt64({high_tx}), toInt64({high_merch})
        ''')
    count = one(client, f'''
        SELECT count()
        FROM marts.daily_metric_publications FINAL
        WHERE run_id = toUUID('{run_id}') AND revision = {revision}
    ''')
    if int(count) != len(dates):
        raise RuntimeError(f'publication check failed: expected {len(dates)} dates, got {count}')
    return revision


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--limit-days', type=int, default=None,
                   help='Non-checkpointing pilot: publish only N latest dirty days')
    args = p.parse_args()
    if args.limit_days is not None and args.limit_days < 1:
        p.error('--limit-days must be >= 1')
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'), port=8123,
        username=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD'], database='default',
        connect_timeout=10, send_receive_timeout=1200,
    )
    ensure_schema(client)
    tx_high = checkpoint(client, 'marts.fact_checkpoints', 'transactions')
    current_tx_high = checkpoint(client, 'staging.current_state_checkpoints', 'transactions')
    merch_high = checkpoint(client, 'staging.current_state_checkpoints', 'merchants')
    merch_staged_high = checkpoint(client, 'staging.staging_checkpoints', 'merchants')
    if min(tx_high, merch_high) < 0:
        raise RuntimeError('Missing fact/merchant checkpoints: complete M3-D first')
    if tx_high != current_tx_high or merch_high != merch_staged_high:
        raise RuntimeError('Upstream checkpoints not aligned: run current-state/fact catch-up first')
    prev_tx, prev_merch = latest_aggregate_progress(client)
    if prev_tx > tx_high or prev_merch > merch_high:
        raise RuntimeError('Aggregate progress exceeds upstream checkpoints')
    # Merchant updates may affect older dates. The reporting view ASOF-resolves
    # merchant history, and dates_to_publish adds merchant-driven dirty dates.
    if prev_tx == tx_high and prev_merch == merch_high and args.limit_days is None:
        print('AGGREGATES_UP_TO_DATE', flush=True)
        return
    dates = dates_to_publish(client, prev_tx, tx_high, prev_merch, merch_high)
    if not dates:
        if args.limit_days is None:
            # No affected reporting dates: advance offsets without a publication.
            client.command(f'''
                INSERT INTO marts.daily_aggregate_checkpoints
                    (run_id, from_transaction_offset, to_transaction_offset,
                     from_merchant_offset, to_merchant_offset, dirty_date_count)
                SELECT generateUUIDv4(), {prev_tx}, {tx_high}, {prev_merch}, {merch_high}, toUInt64(0)
            ''')
        print('AGGREGATES_NO_DIRTY_DATES', flush=True)
        return
    if args.limit_days is not None:
        dates = dates[-args.limit_days:]
    run_id = str(uuid.uuid4())
    print(f'aggregates: {len(dates)} dates, offset {prev_tx}->{tx_high}, '
          f'merchants {prev_merch}->{merch_high}; run_id={run_id}', flush=True)
    insert_metric_rows(client, dates, run_id)
    validate_unpublished(client, dates, run_id)
    # Check upstream progress again: without a scheduler lock, we must not
    # publish a snapshot containing partially overlapping new fact updates.
    if (checkpoint(client, 'marts.fact_checkpoints', 'transactions') != tx_high or
            checkpoint(client, 'staging.current_state_checkpoints', 'merchants') != merch_high):
        raise RuntimeError('Upstream checkpoints advanced during aggregation; retry')
    revision = publish(client, dates, run_id, tx_high, merch_high)
    if args.limit_days is None:
        client.command(f'''
            INSERT INTO marts.daily_aggregate_checkpoints
              (run_id, from_transaction_offset, to_transaction_offset,
               from_merchant_offset, to_merchant_offset, dirty_date_count)
            SELECT toUUID('{run_id}'), {prev_tx}, {tx_high},
                   {prev_merch}, {merch_high}, toUInt64({len(dates)})
        ''')
        print(f'AGGREGATES_CHECKPOINTED tx={tx_high} merchant={merch_high}', flush=True)
    else:
        print('PILOT_ONLY: published latest dirty date; progress checkpoint not advanced', flush=True)
    print(f'DAILY_AGGREGATES_RUN_OK revision={revision} published_dates={len(dates)}', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'DAILY_AGGREGATES_RUN_FAILED: {exc}', file=sys.stderr, flush=True)
        sys.exit(1)
