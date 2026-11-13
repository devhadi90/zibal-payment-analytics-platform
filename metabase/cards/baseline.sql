-- Baseline card queries against Postgres core (definitions: blueprint 3.4).
-- {{AS_OF}} is substituted by the bootstrap/benchmark script with either
--   (now() AT TIME ZONE 'Asia/Tehran')::date      (live dashboard)   or a fixed DATE 'YYYY-MM-DD'  (benchmarks, comparisons)
-- All day/month boundaries are Asia/Tehran; windows are half-open [start, end).

-- name: card1_daily_success_amount_count_90d
SELECT (t.created_at AT TIME ZONE 'Asia/Tehran')::date AS day,
       count(*) AS success_count, sum(t.amount) AS success_amount
FROM core.transactions t
WHERE t.status = 'SUCCESS'
  AND t.created_at >= (({{AS_OF}}) - 89)::timestamp AT TIME ZONE 'Asia/Tehran'
  AND t.created_at <  (({{AS_OF}}) + 1)::timestamp  AT TIME ZONE 'Asia/Tehran'
GROUP BY 1 ORDER BY 1;

-- name: card2_daily_success_rate_by_channel_30d
SELECT (t.created_at AT TIME ZONE 'Asia/Tehran')::date AS day, t.channel,
       count(*) FILTER (WHERE t.status = 'SUCCESS')::numeric
         / NULLIF(count(*) FILTER (WHERE t.status IN ('SUCCESS','FAILED','REVERSED')), 0) AS success_rate
FROM core.transactions t
WHERE t.created_at >= (({{AS_OF}}) - 29)::timestamp AT TIME ZONE 'Asia/Tehran'
  AND t.created_at <  (({{AS_OF}}) + 1)::timestamp  AT TIME ZONE 'Asia/Tehran'
GROUP BY 1, 2 ORDER BY 1, 2;

-- name: card3_top10_merchants_current_month
SELECT m.merchant_id, m.name, sum(t.amount) AS success_amount
FROM core.transactions t JOIN core.merchants m USING (merchant_id)
WHERE t.status = 'SUCCESS'
  AND t.created_at >= date_trunc('month', ({{AS_OF}})::timestamp) AT TIME ZONE 'Asia/Tehran'
  AND t.created_at <  (({{AS_OF}}) + 1)::timestamp AT TIME ZONE 'Asia/Tehran'
GROUP BY m.merchant_id, m.name
ORDER BY success_amount DESC, m.merchant_id ASC
LIMIT 10;

-- name: card4_fee_by_mcc_province_90d   (baseline: merchant's CURRENT mcc/province)
SELECT m.mcc, m.province, sum(t.fee_amount) AS fee_amount
FROM core.transactions t JOIN core.merchants m USING (merchant_id)
WHERE t.status = 'SUCCESS'
  AND t.created_at >= (({{AS_OF}}) - 89)::timestamp AT TIME ZONE 'Asia/Tehran'
  AND t.created_at <  (({{AS_OF}}) + 1)::timestamp  AT TIME ZONE 'Asia/Tehran'
GROUP BY 1, 2 ORDER BY 1, 2;

-- name: card5_monthly_success_by_risk_level   (baseline: merchant's CURRENT risk_level; range = all months in data, to confirm)
SELECT date_trunc('month', t.created_at AT TIME ZONE 'Asia/Tehran')::date AS month, m.risk_level,
       count(*) AS success_count, sum(t.amount) AS success_amount
FROM core.transactions t JOIN core.merchants m USING (merchant_id)
WHERE t.status = 'SUCCESS'
GROUP BY 1, 2 ORDER BY 1, 2;
