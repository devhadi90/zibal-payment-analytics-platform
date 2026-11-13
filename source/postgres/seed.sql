-- Initial data: at least 2,000 merchants and 5M transactions over the preceding 180 days.
-- Status distribution is 80% SUCCESS, 10% FAILED, 10% REVERSED (no PENDING in seed).
-- Override: psql -v rows=3000 -v merchants=20 -f /seed.sql
\set ON_ERROR_STOP on
\if :{?rows}
\else
  \set rows 5000000
\endif
\if :{?merchants}
\else
  \set merchants 2000
\endif

SELECT count(*) > 0 AS seeded FROM core.merchants \gset
\if :seeded
  \echo 'seed previously initialized: checking transaction count'
  SELECT CASE WHEN count(*) < :rows THEN 1 ELSE 0 END AS incomplete FROM core.transactions \gset
  \if :incomplete
    \echo 'ERROR: merchants present but transaction seed incomplete; use make clean to reset development volumes'
    SELECT 1/0 AS seed_is_incomplete;  -- ON_ERROR_STOP makes this fail visibly
  \endif
  \echo 'seed previously initialized: okay'
\else
BEGIN;
SELECT setseed(0.42);
INSERT INTO core.merchants (merchant_id,name,mcc,province,risk_level,owner_national_id,owner_mobile)
SELECT g, 'Merchant ' || g,
  (ARRAY['5411','5812','5732','5651','4121','5999','7399','5942','5311','4900'])[1 + floor(random()*10)::int],
  (ARRAY['Tehran','Isfahan','Fars','Khorasan Razavi','East Azerbaijan','Khuzestan','Mazandaran','Alborz','Gilan','Kerman'])[1 + floor(random()*10)::int],
  (ARRAY['LOW','LOW','LOW','MEDIUM','MEDIUM','HIGH'])[1 + floor(random()*6)::int],
  lpad(floor(random()*1e10)::bigint::text, 10, '0'),
  '09' || lpad(floor(random()*1e9)::bigint::text, 9, '0')
FROM generate_series(1,:merchants) g;

-- Log-uniform amounts 50,000..50,000,000 rials, skewed merchants.
-- No seeded transaction uses 999999 (the temporary test-transaction marker).
INSERT INTO core.transactions
  (transaction_id,merchant_id,channel,card_bin,amount,fee_amount,status,created_at,updated_at)
SELECT id,mid,channel,card_bin,amount,floor(amount*0.012)::bigint,status,
       created_at,least(now(),created_at+(1+random()*5)*interval '1 second')
FROM (
  SELECT g AS id,
    1 + floor(:merchants * power(random(),2))::bigint AS mid,
    (ARRAY['POS','POS','IPG','IPG','IPG','QR'])[1 + floor(random()*6)::int] AS channel,
    (100000 + floor(random()*800000))::int::text AS card_bin,
    (floor(exp(ln(50000)+random()*ln(1000))/1000)*1000)::bigint AS amount,
    CASE WHEN r<0.8 THEN 'SUCCESS' WHEN r<0.9 THEN 'FAILED' ELSE 'REVERSED' END AS status,
    now()-random()*180*interval '1 day' AS created_at
  FROM (SELECT g,random() AS r FROM generate_series(1,:rows) g) x
) seeded;
SELECT setval(pg_get_serial_sequence('core.transactions','transaction_id'),
              (SELECT max(transaction_id) FROM core.transactions));
COMMIT;
\endif

-- Baseline indexes are reasonable and recorded: no artificial source slowdown.
CREATE INDEX IF NOT EXISTS transactions_created_at_idx ON core.transactions(created_at);
CREATE INDEX IF NOT EXISTS transactions_merchant_idx ON core.transactions(merchant_id,created_at);
ANALYZE core.merchants;
ANALYZE core.transactions;
\echo 'seed complete'
