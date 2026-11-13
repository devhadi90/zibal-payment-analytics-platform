#!/usr/bin/env python3
"""M5-B: strictly reconcile each Tehran-local successful-sales day, PG vs published CH.

Read-only Metabase API execution; no database writes. Requires M4-B benchmark_m4.py
and the existing M1 metabase/bootstrap.py in the same directory.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys

import benchmark_m4 as bm

REPORTS = Path('/reports')
PG_NAME = 'Zibal source PostgreSQL'
CH_NAME = 'Zibal analytical ClickHouse'


def pg_sql(start: date, end: date) -> str:
    # Using Tehran midnight as timestamptz bounds also handles DST if applicable.
    next_day = end + timedelta(days=1)
    return f"""
WITH source_daily AS (
    SELECT (t.created_at AT TIME ZONE 'Asia/Tehran')::date AS day,
           count(*)::bigint AS success_count,
           sum(t.amount)::numeric AS success_amount
    FROM core.transactions AS t
    WHERE t.status = 'SUCCESS'
      AND t.created_at >= (TIMESTAMP '{start} 00:00:00' AT TIME ZONE 'Asia/Tehran')
      AND t.created_at <  (TIMESTAMP '{next_day} 00:00:00' AT TIME ZONE 'Asia/Tehran')
    GROUP BY 1
)
SELECT g.day::date AS day,
       coalesce(s.success_count, 0)::bigint AS success_count,
       coalesce(s.success_amount, 0)::numeric AS success_amount
FROM generate_series(TIMESTAMP '{start} 00:00:00',
                     TIMESTAMP '{end} 00:00:00',
                     INTERVAL '1 day') AS g(day)
LEFT JOIN source_daily AS s ON s.day = g.day::date
ORDER BY day
""".strip()


def ch_sql(start: date, end: date) -> str:
    # Publication is the commit marker. A published zero-SUCCESS day is valid.
    return f"""
SELECT p.date AS day,
       ifNull(s.success_count, 0) AS success_count,
       ifNull(s.success_amount, 0) AS success_amount
FROM
(
    SELECT date
    FROM marts.daily_metric_publications FINAL
    WHERE date >= toDate('{start}') AND date <= toDate('{end}')
) AS p
LEFT JOIN
(
    SELECT date, sum(success_count) AS success_count,
           sum(success_amount) AS success_amount
    FROM marts.agg_daily_sales
    WHERE date >= toDate('{start}') AND date <= toDate('{end}')
    GROUP BY date
) AS s ON p.date = s.date
ORDER BY day
""".strip()


def normalize(rows: list[dict], label: str):
    out = {}
    for record in rows:
        for name in ('day', 'success_count', 'success_amount'):
            if name not in record or record[name] is None:
                raise ValueError(f'{label}: missing/null {name} in {record!r}')
        day = bm.key_component(record['day'])
        date.fromisoformat(day)  # Reject unexpected day formats.
        if day in out:
            raise ValueError(f'{label}: duplicate day {day}')
        count, amount = bm.as_decimal(record['success_count']), bm.as_decimal(record['success_amount'])
        if count != count.to_integral_value() or amount != amount.to_integral_value():
            raise ValueError(f'{label}: non-integral counts/amounts on {day}')
        out[day] = (count, amount)
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', default='2026-05-15', help='first Tehran-local day, YYYY-MM-DD')
    parser.add_argument('--end', default='2026-11-13', help='last Tehran-local day, YYYY-MM-DD')
    args = parser.parse_args()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    expected_dates = [(start + timedelta(days=n)).isoformat()
                      for n in range((end - start).days + 1)]
    if not expected_dates or len(expected_dates) > 2000:
        raise ValueError('Empty or >2000-day window; choose a bounded date range')

    bm.mb.wait_healthy()
    session = bm.mb.login_or_setup()
    dbs = {d['name']: d for d in bm.mb.collection(bm.mb.request('/api/database', session=session))}
    for name, engine in ((PG_NAME, 'postgres'), (CH_NAME, 'clickhouse')):
        if name not in dbs or dbs[name].get('engine') != engine:
            raise RuntimeError(f'Missing/incorrect Metabase database {name}: expected {engine}')
    pg_id, ch_id = dbs[PG_NAME]['id'], dbs[CH_NAME]['id']
    print(f'M5B start={start} end={end} expected_days={len(expected_dates)} '
          f'PG_db={pg_id} CH_db={ch_id}', flush=True)

    pg_ms, pg_rows, pc, pdc = bm.execute(session, pg_id, pg_sql(start, end))
    ch_ms, ch_rows, cc, cdc = bm.execute(session, ch_id, ch_sql(start, end))
    if any(flag is True for flag in (pc, pdc, cc, cdc)):
        raise RuntimeError('Metabase returned a cached query result; disable caching before reconciliation')
    pg, ch = normalize(pg_rows, 'PostgreSQL'), normalize(ch_rows, 'ClickHouse')
    dates = set(expected_dates)
    missing_pg = sorted(dates - set(pg))
    missing_ch = sorted(dates - set(ch))
    extra_pg = sorted(set(pg) - dates)
    extra_ch = sorted(set(ch) - dates)
    mismatches = [
        {'day': day,
         'pg_count': str(pg[day][0]), 'ch_count': str(ch[day][0]),
         'count_delta_ch_minus_pg': str(ch[day][0] - pg[day][0]),
         'pg_amount': str(pg[day][1]), 'ch_amount': str(ch[day][1]),
         'amount_delta_ch_minus_pg': str(ch[day][1] - pg[day][1])}
        for day in expected_dates if day in pg and day in ch and pg[day] != ch[day]
    ]
    pg_count = sum((x[0] for x in pg.values()), Decimal(0))
    ch_count = sum((x[0] for x in ch.values()), Decimal(0))
    pg_amount = sum((x[1] for x in pg.values()), Decimal(0))
    ch_amount = sum((x[1] for x in ch.values()), Decimal(0))
    passed = not (missing_pg or missing_ch or extra_pg or extra_ch or mismatches)
    result = {
        'checked_at_utc': datetime.now(timezone.utc).isoformat(),
        'date_basis': 'Asia/Tehran', 'start': str(start), 'end': str(end),
        'expected_days': len(expected_dates), 'passed': passed,
        'source': {'database': PG_NAME, 'metabase_id': pg_id, 'days': len(pg),
                   'success_count': str(pg_count), 'success_amount_irr': str(pg_amount),
                   'query_ms': pg_ms},
        'target': {'database': CH_NAME, 'metabase_id': ch_id, 'days': len(ch),
                   'success_count': str(ch_count), 'success_amount_irr': str(ch_amount),
                   'query_ms': ch_ms},
        'delta_ch_minus_pg': {'success_count': str(ch_count - pg_count),
                              'success_amount_irr': str(ch_amount - pg_amount)},
        'missing_source_days': missing_pg, 'missing_published_days': missing_ch,
        'unexpected_source_days': extra_pg, 'unexpected_published_days': extra_ch,
        'mismatch_count': len(mismatches), 'mismatches': mismatches,
        'limitation': 'Read-only snapshot comparison; simulator/other writers must be stopped and CDC/fact/aggregate checkpoints caught up before running.',
    }
    REPORTS.mkdir(parents=True, exist_ok=True)
    dest = REPORTS / 'm5b_daily_reconciliation.json'
    dest.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f'PostgreSQL: {len(pg)} days, count={pg_count}, amount={pg_amount} IRR', flush=True)
    print(f'ClickHouse: {len(ch)} days, count={ch_count}, amount={ch_amount} IRR', flush=True)
    print(f'Missing published days={len(missing_ch)}, mismatched days={len(mismatches)}', flush=True)
    for diff in mismatches[:5]:
        print(f"DIFF {diff['day']}: count delta={diff['count_delta_ch_minus_pg']} "
              f"amount delta={diff['amount_delta_ch_minus_pg']} IRR", flush=True)
    print(f'WROTE {dest}', flush=True)
    if not passed:
        raise RuntimeError('M5B_RECONCILIATION_FAILED — see report for per-day discrepancies')
    print(f'M5B_RECONCILIATION_OK days={len(expected_dates)} count_delta=0 amount_delta=0', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'M5B_FAILED: {exc}', file=sys.stderr, flush=True)
        sys.exit(1)
