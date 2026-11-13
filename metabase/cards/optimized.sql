-- M4 optimized questions on ClickHouse daily published marts.
-- {{AS_OF}} is compiled by metabase/optimized.py to a Tehran-local Date.
-- Baseline definitions: metabase/cards/baseline.sql, blueprint v3.1 §3.4.
-- Rolling windows INCLUDE as-of date; current month = Tehran Gregorian month.

-- name: card1_daily_success_amount_count_90d
SELECT date AS day,
       sum(success_count) AS success_count,
       sum(success_amount) AS success_amount
FROM marts.agg_daily_sales
WHERE date >= addDays({{AS_OF}}, -89)
  AND date <= {{AS_OF}}
GROUP BY day
ORDER BY day;

-- name: card2_daily_success_rate_by_channel_30d
SELECT date AS day,
       channel,
       toFloat64(sum(success_count)) / nullIf(toFloat64(sum(terminal_count)), 0) AS success_rate
FROM marts.agg_daily_channel
WHERE date >= addDays({{AS_OF}}, -29)
  AND date <= {{AS_OF}}
GROUP BY day, channel
ORDER BY day, channel;

-- name: card3_top10_merchants_current_month
SELECT a.merchant_id,
       m.name,
       a.success_amount
FROM
(
    SELECT merchant_id, sum(success_amount) AS success_amount
    FROM marts.agg_daily_merchant
    WHERE date >= toStartOfMonth({{AS_OF}})
      AND date <= {{AS_OF}}
    GROUP BY merchant_id
    ORDER BY success_amount DESC, merchant_id ASC
    LIMIT 10
) AS a
INNER JOIN
(
    SELECT merchant_id, name
    FROM marts.dim_merchant_history
    WHERE is_current = 1
) AS m ON a.merchant_id = m.merchant_id
ORDER BY a.success_amount DESC, a.merchant_id ASC;

-- name: card4_fee_by_mcc_province_90d
SELECT mcc,
       province,
       sum(fee_amount) AS fee_amount
FROM marts.agg_daily_fee_mcc_province
WHERE date >= addDays({{AS_OF}}, -89)
  AND date <= {{AS_OF}}
GROUP BY mcc, province
ORDER BY mcc, province;

-- name: card5_monthly_success_by_risk_level
SELECT toStartOfMonth(date) AS month,
       risk_level,
       sum(success_count) AS success_count,
       sum(success_amount) AS success_amount
FROM marts.agg_daily_risk
GROUP BY month, risk_level
ORDER BY month, risk_level;
