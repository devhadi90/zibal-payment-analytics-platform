"""T0 read-only source verifier. Reports observations, not guessed benchmark numbers."""
import argparse
import json
import os
import time
import psycopg

parser = argparse.ArgumentParser()
parser.add_argument("--observe-tps", type=int, default=0,
                    help="Observe net new rows over N seconds (600 recommended)")
args = parser.parse_args()
min_merchants = int(os.getenv("SEED_MERCHANTS", "2000"))
min_rows = int(os.getenv("SEED_ROWS", "5000000"))

with psycopg.connect() as conn:
    merchants = conn.execute("SELECT count(*) FROM core.merchants").fetchone()[0]
    total, earliest, latest = conn.execute(
        "SELECT count(*), min(created_at), max(created_at) FROM core.transactions"
    ).fetchone()
    status_counts = dict(conn.execute(
        "SELECT status, count(*) FROM core.transactions GROUP BY status"
    ).fetchall())
    tables = dict(conn.execute(
        "SELECT n.nspname || '.' || c.relname, c.relreplident "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='core' AND c.relname IN ('transactions','merchants')"
    ).fetchall())
    metabase_db = conn.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname='metabase_app')"
    ).fetchone()[0]
    jobs = conn.execute("SELECT count(*) FROM simulator.pending_actions").fetchone()[0]
    status_ok = all(abs(status_counts.get(s, 0) / total - p) < .02 for s, p in
                    (("SUCCESS", .8), ("FAILED", .1), ("REVERSED", .1))) if total else False
    historical_days = (latest - earliest).total_seconds() / 86400 if earliest and latest else 0
    valid = (
        merchants >= min_merchants and total >= min_rows and
        set(tables) == {'core.merchants', 'core.transactions'} and
        all(v == 'f' for v in tables.values()) and metabase_db and
        historical_days >= 179 and status_ok
    )
    evidence = {"merchants": merchants, "transactions": total,
                "min_created_at": earliest.isoformat() if earliest else None,
                "max_created_at": latest.isoformat() if latest else None,
                "status_counts": status_counts,
                "current_status_pct": {k: round(100*v/total, 3) for k, v in status_counts.items()},
                "replica_identity_full": tables,
                "metabase_app_db_exists": metabase_db,
                "pending_simulator_actions": jobs,
                "history_span_days": round(historical_days, 2),
                "status_distribution_near_80_10_10": status_ok,
                "status": "PASS" if valid else "FAIL"}
    print(json.dumps(evidence, indent=2))
    if not valid:
        raise SystemExit(1)

if args.observe_tps:
    with psycopg.connect() as conn:
        first = conn.execute("SELECT count(*) FROM core.transactions").fetchone()[0]
        print(f"Measuring net new source rows for {args.observe_tps} seconds...", flush=True)
        time.sleep(args.observe_tps)
        last = conn.execute("SELECT count(*) FROM core.transactions").fetchone()[0]
    actual = (last - first) / args.observe_tps
    expected = float(os.getenv("SIM_TPS", "20"))
    # The temporary test transactions (2% by default) are deleted shortly after insert.
    # Net row growth is therefore slightly lower than gross inserts.
    ok = abs(actual - expected) <= max(expected*.1, 1)
    print(json.dumps({"net_rows_added": last-first, "seconds": args.observe_tps,
                      "net_tps": round(actual, 2), "configured_tps": expected,
                      "status": "PASS" if ok else "FAIL"}, indent=2))
    if not ok:
        raise SystemExit(1)
