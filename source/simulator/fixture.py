"""Deterministic temporal fixture (blueprint T3-03): captures real DB-clock timestamps.
merchant LOW/Tehran at t0; txn A at t1; merchant -> HIGH/Fars at t2; txn B at t3; A -> SUCCESS at t4."""
import argparse, json, time
import psycopg

ap = argparse.ArgumentParser()
ap.add_argument("--merchant-id", type=int, default=9000001)
a = ap.parse_args()
step = lambda: time.sleep(0.05)
with psycopg.connect(autocommit=True) as c:
    t0 = c.execute("INSERT INTO core.merchants (merchant_id,name,mcc,province,risk_level,owner_national_id,owner_mobile) "
                   "VALUES (%s,'Fixture merchant','5411','Tehran','LOW','0000000000','09000000000') RETURNING updated_at", (a.merchant_id,)).fetchone()[0]; step()
    ins = ("INSERT INTO core.transactions (merchant_id,channel,card_bin,amount,fee_amount,status) "
           "VALUES (%s,'POS','123456',100000,1200,'PENDING') RETURNING transaction_id, created_at")
    id_a, t1 = c.execute(ins, (a.merchant_id,)).fetchone(); step()
    t2 = c.execute("UPDATE core.merchants SET risk_level='HIGH', province='Fars' WHERE merchant_id=%s RETURNING updated_at", (a.merchant_id,)).fetchone()[0]; step()
    id_b, t3 = c.execute(ins, (a.merchant_id,)).fetchone(); step()
    t4 = c.execute("UPDATE core.transactions SET status='SUCCESS' WHERE transaction_id=%s RETURNING updated_at", (id_a,)).fetchone()[0]
assert t0 < t1 < t2 < t3 < t4, "timestamp ordering violated"
print(json.dumps({"merchant_id": a.merchant_id, "txn_a": id_a, "txn_b": id_b,
                  "t0": t0.isoformat(), "t1": t1.isoformat(), "t2": t2.isoformat(),
                  "t3": t3.isoformat(), "t4": t4.isoformat()}, indent=2))
