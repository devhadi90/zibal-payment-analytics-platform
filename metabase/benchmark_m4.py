#!/usr/bin/env python3
"""M4-B: read-only PostgreSQL vs ClickHouse Metabase dataset comparison/benchmark.

Runs exactly the SQL templates behind existing dashboards, with a shared fixed
Tehran-local date. No saved Metabase objects, database contents or cache settings
are changed. Benchmark latency is wall-clock HTTP/API+query, not SQL-only.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import statistics
import sys
import time

import bootstrap as mb
import optimized as opt

BASE = Path(__file__).resolve().parent
REPORTS = Path('/reports')
DB_NAMES = ('Zibal source PostgreSQL', 'Zibal analytical ClickHouse')
DISPLAY_NAMES = ('PostgreSQL', 'ClickHouse')
CARD_KEYS = (
    ('day',),
    ('day', 'channel'),
    ('merchant_id',),
    ('mcc', 'province'),
    ('month', 'risk_level'),
)
CARD_METRICS = (
    ('success_count', 'success_amount'),
    ('success_rate',),
    ('success_amount',),
    ('fee_amount',),
    ('success_count', 'success_amount'),
)


def parse_pg():
    return mb.parse_cards(BASE / 'cards' / 'baseline.sql')


def compile_pg(query, as_of):
    date.fromisoformat(as_of)
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', as_of):
        raise ValueError('Invalid fixed as-of date')
    compiled = query.replace('{{AS_OF}}', f"DATE '{as_of}'")
    if '{{' in compiled or '}}' in compiled:
        raise ValueError('Unresolved marker in baseline SQL')
    return compiled


def parse_and_compile(as_of):
    baseline = parse_pg()
    improved = opt.cards()
    if len(baseline) != 5 or [x[0] for x in baseline] != [x[0] for x in improved]:
        raise ValueError('Baseline/optimized five card identifiers differ')
    return [
        (baseline[i][0], compile_pg(baseline[i][1], as_of),
         opt.compile_query(improved[i][1], as_of)) for i in range(5)
    ]


def execute(session, db_id, sql):
    start = time.perf_counter()
    resp = mb.request('/api/dataset', 'POST', {
        'database': db_id, 'type': 'native', 'native': {'query': sql}
    }, session=session, timeout=180)
    elapsed = round((time.perf_counter() - start) * 1000, 2)
    if resp.get('status') == 'failed' or resp.get('error'):
        raise RuntimeError(f"Metabase database {db_id} failed: {resp.get('error')}")
    data = resp.get('data', {})
    rows, cols = data.get('rows'), data.get('cols')
    if not isinstance(rows, list) or not isinstance(cols, list):
        raise RuntimeError(f'Unexpected API dataset payload from db={db_id}')
    names = [c.get('name', '').lower() for c in cols]
    if not all(names) or len(set(names)) != len(names):
        raise RuntimeError(f'Invalid column labels: {names}')
    return elapsed, [dict(zip(names, row)) for row in rows], resp.get('cached'), data.get('cached')


def key_component(value):
    if value is None:
        return ''
    s = str(value)
    # Both PG and ClickHouse may serialize a Date as local ISO midnight
    if re.match(r'^\d{4}-\d{2}-\d{2}(?:T| |$)', s):
        return s[:10]
    return s


def as_decimal(value):
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation as e:
        raise ValueError(f'Cannot compare numeric value {value!r}') from e


def rows_map(rows, keys, metrics):
    result = {}
    for row in rows:
        if not set(keys + metrics).issubset(row):
            raise ValueError(f"Missing expected fields: {set(keys + metrics) - set(row)}")
        key = tuple(key_component(row[k]) for k in keys)
        if key in result:
            raise ValueError(f'Duplicate result key {key}')
        result[key] = {m: as_decimal(row[m]) for m in metrics}
    return result


def totals(mapping, metrics):
    return {m: sum((v[m] for v in mapping.values() if v[m] is not None), Decimal(0))
            for m in metrics}


def compare_card(idx, name, left, right):
    keys, metrics = CARD_KEYS[idx], CARD_METRICS[idx]
    pg = rows_map(left, keys, metrics)
    ch = rows_map(right, keys, metrics)
    common = set(pg) & set(ch)
    only_pg, only_ch = sorted(set(pg) - set(ch)), sorted(set(ch) - set(pg))
    diff_count = 0
    samples = []
    for k in sorted(common):
        for metric in metrics:
            a, b = pg[k][metric], ch[k][metric]
            tolerance = Decimal('0.000000001') if metric == 'success_rate' else Decimal('0')
            equal = (a is None and b is None) or (
                a is not None and b is not None and abs(a - b) <= tolerance
            )
            if not equal:
                diff_count += 1
                if len(samples) < 3:
                    samples.append({'key': k, 'metric': metric, 'pg': str(a), 'ch': str(b)})
    total_pg, total_ch = totals(pg, metrics), totals(ch, metrics)
    totals_equal = all(total_pg[m] == total_ch[m] for m in metrics if m != 'success_rate')
    strict_equal = not only_pg and not only_ch and diff_count == 0
    # Card 4 and 5 have intentionally different dimensions: PG = current merchant,
    # CH = transaction-time SCD2. Totals must still reconcile exactly.
    expected = strict_equal if idx in (0, 1, 2) else totals_equal
    return {
        'card': name, 'pg_rows': len(left), 'ch_rows': len(right),
        'strict_equal': strict_equal,
        'passed': expected,
        'comparison_rule': 'strict_rows' if idx < 3 else 'grand_totals_historical_dimensional_difference_allowed',
        'pg_only_keys': len(only_pg), 'ch_only_keys': len(only_ch),
        'mismatched_metric_cells': diff_count, 'examples': samples,
        'pg_totals': {k: str(v) for k, v in total_pg.items()},
        'ch_totals': {k: str(v) for k, v in total_ch.items()},
    }


def maybe_caching_setting(session):
    try:
        setting = mb.request('/api/setting/enable-query-caching', session=session, timeout=20)
        if isinstance(setting, dict):
            return setting.get('value', setting)
        return setting
    except Exception as exc:
        return f'unavailable ({type(exc).__name__})'


def quantile_95(values):
    ordered = sorted(values)
    return ordered[max(0, (95 * len(ordered) + 99) // 100 - 1)]


def resultfile(filename, payload):
    REPORTS.mkdir(parents=True, exist_ok=True)
    path = REPORTS / filename
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f'WROTE {path}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--check', action='store_true', help='one-pass result comparison only')
    mode.add_argument('--benchmark', action='store_true', help='compare then measure both systems')
    parser.add_argument('--as-of', default='2026-11-13', help='fixed Tehran-local YYYY-MM-DD; default 2026-11-13')
    parser.add_argument('--runs', type=int, default=20, help='repetitions per card and engine (>=20 for final)')
    parser.add_argument('--warmups', type=int, default=1, help='warm-up runs per card and engine')
    args = parser.parse_args()
    if args.runs < 1 or args.warmups < 0:
        raise ValueError('--runs must be positive and warmups nonnegative')
    queries = parse_and_compile(args.as_of)
    mb.wait_healthy()
    session = mb.login_or_setup()
    dbs = {d['name']: d for d in mb.collection(mb.request('/api/database', session=session))}
    if not all(name in dbs for name in DB_NAMES):
        raise RuntimeError('Missing PostgreSQL/ClickHouse Metabase database connections')
    ids = [dbs[n]['id'] for n in DB_NAMES]
    if dbs[DB_NAMES[0]].get('engine') != 'postgres' or dbs[DB_NAMES[1]].get('engine') != 'clickhouse':
        raise RuntimeError('Metabase engine types are not PostgreSQL and ClickHouse')
    print(f'M4B fixed_as_of={args.as_of}, DB IDs={ids}, Metabase cache enabled setting={maybe_caching_setting(session)}', flush=True)
    comparisons = []
    first_cache_observed = []
    for idx, (name, pg_sql, ch_sql) in enumerate(queries):
        _, pg_rows, p_cached, p_data_cached = execute(session, ids[0], pg_sql)
        _, ch_rows, c_cached, c_data_cached = execute(session, ids[1], ch_sql)
        first_cache_observed.extend([p_cached, p_data_cached, c_cached, c_data_cached])
        record = compare_card(idx, name, pg_rows, ch_rows)
        comparisons.append(record)
        print(f"{'PASS' if record['passed'] else 'FAIL'} {idx+1}/5 {name}: PG={record['pg_rows']} CH={record['ch_rows']} "
              f"strict={record['strict_equal']}, rule={record['comparison_rule']}, differing_cells={record['mismatched_metric_cells']}", flush=True)
    comparison_payload = {
        'measured_utc': datetime.now(timezone.utc).isoformat(), 'as_of_date': args.as_of,
        'data_cutoff': 'simulator stopped; manual source/mart checkpoint reconciliation required',
        'cached_result_flags': first_cache_observed,
        'results': comparisons,
    }
    resultfile('m4b_comparison.json', comparison_payload)
    if not all(r['passed'] for r in comparisons):
        raise RuntimeError('M4B_COMPARE_FAILED — inspect /reports/m4b_comparison.json; benchmark not started')
    print('M4B_COMPARE_OK', flush=True)
    if args.check:
        return

    if args.runs < 20:
        print('WARNING: fewer than 20 repetitions do not satisfy final T4 benchmark requirement', flush=True)
    measures = {(engine, i): [] for engine in (0, 1) for i in range(5)}
    all_cached_flags = []
    for j in range(args.warmups):
        for idx, (_, pg_sql, ch_sql) in enumerate(queries):
            for engine, sql in ((0, pg_sql), (1, ch_sql)):
                execute(session, ids[engine], sql)
        print(f'warmup {j+1}/{args.warmups} complete', flush=True)
    # Interleave PG and CH to avoid measuring two systems at different times.
    # Alternate engine order each iteration to reduce order bias.
    for iteration in range(args.runs):
        for idx, (name, pg_sql, ch_sql) in enumerate(queries):
            pair = [(0, pg_sql), (1, ch_sql)]
            if (iteration + idx) % 2:
                pair.reverse()
            for engine, sql in pair:
                ms, rows, flag, data_flag = execute(session, ids[engine], sql)
                measures[(engine, idx)].append(ms)
                all_cached_flags.extend([flag, data_flag])
        print(f'benchmark iteration {iteration+1}/{args.runs} finished', flush=True)
    if any(flag is True for flag in all_cached_flags):
        raise RuntimeError('Metabase reported cached results: benchmark not valid')
    cards = []
    for idx, (name, pg_sql, ch_sql) in enumerate(queries):
        a, b = measures[(0, idx)], measures[(1, idx)]
        pg_median, ch_median = statistics.median(a), statistics.median(b)
        cards.append({
            'card': name,
            'pg': {'samples_ms': a, 'median_ms': round(pg_median, 2), 'p95_ms': quantile_95(a)},
            'ch': {'samples_ms': b, 'median_ms': round(ch_median, 2), 'p95_ms': quantile_95(b)},
            'median_speedup_pg_over_ch': round(pg_median / ch_median, 2) if ch_median else None,
            'baseline_sql': pg_sql, 'optimized_sql': ch_sql,
        })
        print(f'{name}: PG median={pg_median:.2f}ms, CH median={ch_median:.2f}ms, speedup={pg_median/ch_median:.2f}x', flush=True)
    payload = {
        'measured_utc': datetime.now(timezone.utc).isoformat(),
        'as_of_date': args.as_of, 'runs': args.runs, 'warmups': args.warmups,
        'via': 'Metabase POST /api/dataset (native SQL, request+query wall clock; not saved card dashboard rendering)',
        'cache_policy': 'ad-hoc native SQL requests; cache setting measured in preflight; cached result flags checked where exposed',
        'caveat': 'Metabase response time includes API overhead. Cache-hit flags are not guaranteed in all Metabase versions.',
        'comparison': comparisons, 'cards': cards
    }
    resultfile('m4b_benchmark.json', payload)
    print('M4B_BENCHMARK_OK', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'M4B_FAILED: {e}', file=sys.stderr, flush=True)
        sys.exit(1)
