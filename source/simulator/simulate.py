"""Continuous source simulation at ~20 new PENDING transactions / second.

Future actions live in simulator.pending_actions and are committed in the same
Postgres transaction as the source write.  A container restart does not discard
scheduled settlements, reversals, or hard deletes.  The queue is NOT captured by CDC.
"""
import argparse
import random
import signal
import time

import psycopg

CHANNELS = ["POS", "POS", "IPG", "IPG", "IPG", "QR"]
PROVINCES = ["Tehran", "Isfahan", "Fars", "Khorasan Razavi", "East Azerbaijan",
             "Khuzestan", "Mazandaran", "Alborz", "Gilan", "Kerman"]
RISKS = ["LOW", "MEDIUM", "HIGH"]
TEST_BIN = "999999"  # seed never generates this; only test rows have this value
STOP = False


def stop_handler(*_):
    global STOP
    STOP = True


def connect():
    while not STOP:
        try:
            return psycopg.connect(autocommit=True)  # PGHOST, PGUSER, etc.
        except psycopg.OperationalError as exc:
            print(f"postgres unavailable: {exc}; retrying", flush=True)
            time.sleep(2)
    raise RuntimeError("simulator stopped")


def has_installments(conn):
    return conn.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
        "WHERE table_schema='core' AND table_name='transactions' AND column_name='installments')"
    ).fetchone()[0]


def new_transactions(conn, rnd, n_merchants, tps, test_ratio, installments):
    rows = []
    for _ in range(tps):
        is_test = rnd.random() < test_ratio
        amount = 1000 if is_test else (int(50000 * 1000 ** rnd.random()) // 1000) * 1000
        rows.append((1 + int(n_merchants * rnd.random() ** 2), rnd.choice(CHANNELS),
                     TEST_BIN if is_test else str(rnd.randint(100000, 899999)),
                     amount, int(amount * .012),
                     rnd.randint(1, 12) if installments and rnd.random() < .3 else None,
                     is_test, rnd.uniform(10, 60) if is_test else rnd.uniform(2, 6)))
    cols = "merchant_id, channel, card_bin, amount, fee_amount, status"
    fields = "%s,%s,%s,%s,%s,'PENDING'"
    if installments:
        cols += ", installments"
        fields += ",%s"
    q = f"INSERT INTO core.transactions ({cols}) VALUES " + ",".join("(" + fields + ")" for _ in rows)
    q += " RETURNING transaction_id"
    params = [p for row in rows for p in (row[:5] + ((row[5],) if installments else ()))]
    with conn.transaction():
        ids = [r[0] for r in conn.execute(q, params).fetchall()]
        jobs = [(tid, "test_delete" if row[6] else "settle", row[7]) for tid, row in zip(ids, rows)]
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO simulator.pending_actions(transaction_id, action, due_at) "
                "VALUES (%s,%s,clock_timestamp() + make_interval(secs => %s))", jobs
            )
    return len(ids)


def process_due(conn, rnd, success_ratio, reverse_ratio):
    # Source changes AND their queue acknowledgments share one DB transaction.
    with conn.transaction():
        jobs = conn.execute(
            "SELECT transaction_id, action FROM simulator.pending_actions "
            "WHERE due_at <= clock_timestamp() ORDER BY due_at "
            "LIMIT 500 FOR UPDATE SKIP LOCKED"
        ).fetchall()
        if not jobs:
            return 0
        settles = [tid for tid, kind in jobs if kind == "settle"]
        reverses = [tid for tid, kind in jobs if kind == "reverse"]
        deletes = [tid for tid, kind in jobs if kind == "test_delete"]
        winners = [tid for tid in settles if rnd.random() < success_ratio]
        winner_set = set(winners)
        losers = [tid for tid in settles if tid not in winner_set]

        if winners:
            conn.execute("UPDATE core.transactions SET status='SUCCESS' "
                         "WHERE transaction_id = ANY(%s) AND status='PENDING'", (winners,))
        if losers:
            conn.execute("UPDATE core.transactions SET status='FAILED' "
                         "WHERE transaction_id = ANY(%s) AND status='PENDING'", (losers,))
        if reverses:
            conn.execute("UPDATE core.transactions SET status='REVERSED' "
                         "WHERE transaction_id = ANY(%s) AND status='SUCCESS'", (reverses,))
        if deletes:
            conn.execute("DELETE FROM core.transactions WHERE transaction_id = ANY(%s) "
                         "AND card_bin = %s", (deletes, TEST_BIN))
        ids = [tid for tid, _ in jobs]
        conn.execute("DELETE FROM simulator.pending_actions WHERE transaction_id = ANY(%s)", (ids,))
        reverse_jobs = [(tid, rnd.uniform(30, 300)) for tid in winners if rnd.random() < reverse_ratio]
        if reverse_jobs:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO simulator.pending_actions(transaction_id,action,due_at) "
                    "VALUES (%s,'reverse',clock_timestamp()+make_interval(secs => %s))", reverse_jobs
                )
    return len(jobs)


def change_merchant(conn, rnd, n_merchants):
    mid = rnd.randint(1, n_merchants)
    with conn.transaction():
        risk, province = conn.execute(
            "SELECT risk_level,province FROM core.merchants WHERE merchant_id=%s FOR UPDATE", (mid,)
        ).fetchone()
        conn.execute(
            "UPDATE core.merchants SET risk_level=%s,province=%s WHERE merchant_id=%s",
            (rnd.choice([r for r in RISKS if r != risk]),
             rnd.choice([p for p in PROVINCES if p != province]), mid)
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tps", type=int, default=20)
    p.add_argument("--seed", type=int)
    p.add_argument("--test-ratio", type=float, default=.02)
    p.add_argument("--success-ratio", type=float, default=.88)
    p.add_argument("--reverse-ratio", type=float, default=.05)
    p.add_argument("--merchant-interval", type=float, default=60)
    p.add_argument("--duration", type=float, default=0, help="Seconds; 0 means run continuously")
    args = p.parse_args()
    if args.tps < 1 or args.merchant_interval <= 0 or any(
        not 0 <= n <= 1 for n in [args.test_ratio, args.success_ratio, args.reverse_ratio]
    ):
        p.error("tps/interval must be positive and ratios must be in [0,1]")
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, stop_handler)
    rnd = random.Random(args.seed)
    conn = connect()
    n_merchants = conn.execute(
        "SELECT count(*) FROM core.merchants WHERE merchant_id BETWEEN 1 AND 8999999"
    ).fetchone()[0]
    if not n_merchants:
        raise RuntimeError("no merchants; seed must finish before the simulator starts")

    start = tick = time.monotonic()
    next_merchant = start + args.merchant_interval
    next_schema = start
    has_inst = False
    inserted = completed = 0
    log_at = start + 10
    while not STOP and (not args.duration or time.monotonic()-start < args.duration):
        try:
            now = time.monotonic()
            if now >= next_schema:
                has_inst = has_installments(conn)
                next_schema = now + 5
            inserted += new_transactions(conn, rnd, n_merchants, args.tps, args.test_ratio, has_inst)
            completed += process_due(conn, rnd, args.success_ratio, args.reverse_ratio)
            if now >= next_merchant:
                change_merchant(conn, rnd, n_merchants)
                next_merchant = now + args.merchant_interval
            if now >= log_at:
                pending = conn.execute("SELECT count(*) FROM simulator.pending_actions").fetchone()[0]
                print(f"inserted={inserted} completed_actions={completed} "
                      f"pending_actions={pending} installments={has_inst}", flush=True)
                log_at = now + 10
        except psycopg.OperationalError as exc:
            print(f"DB connection lost, reconnecting: {exc}", flush=True)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()
            # Durable pending_actions are retained; no blind status updates are necessary.
        tick = max(tick + 1, time.monotonic())
        time.sleep(max(0, tick - time.monotonic()))
    conn.close()
    print(f"stopped: inserted={inserted} processed_actions={completed}", flush=True)


if __name__ == "__main__":
    main()
