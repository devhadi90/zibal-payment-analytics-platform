# Data ownership and sensitivity (Zibal exercise)

**Ownership is role-based:** the fictional exercise has no assigned production owner names. Proposed accountable role: **Data Engineering** (pipeline, reconciliation and marts), with **Finance/BI** defining and approving reporting KPIs. Source-of-record ownership belongs to the **Transactional Application** team. These are assignments for the take-home prototype, not claims about Zibal's actual organization.

| Relation(s) | Responsible role | Sensitivity | Reporting exposure / precautions |
|---|---|---|---|
| `core.merchants` (PostgreSQL) | Transactional Application | **Restricted PII**: `owner_national_id`, `owner_mobile`; merchant risk | Source only; baseline restricted read-only service role |
| `core.transactions` (PostgreSQL) | Transactional Application | Financial amounts, `card_bin`, merchant IDs | Source; read-only baseline role |
| `raw.cdc_merchants`, `raw.cdc_transactions` (ClickHouse) | Data Engineering | **Restricted**: unmasked PII in merchant before/after JSON; event before-images | Not granted to `zibal_reporting`; access requires controlled administrative credentials |
| `staging.stg_merchants_events`, `staging.stg_merchants_current` | Data Engineering | **Masked PII**: `owner_national_id_masked`, `owner_mobile_masked`; sensitive merchant risk | No plaintext identifiers; avoid reporting access to masked columns; M4 permissions on staging events exist as a dependency of regular SCD2 views |
| `staging.stg_transactions_events`, `staging.stg_transactions_current` | Data Engineering | Internal financial + payment metadata (`card_bin`) | Transform-only access |
| `marts.dim_merchant_history` | Data Engineering / BI | Merchant ID, name, MCC, province, and historical risk | Read-only BI; baseline pre-snapshot version is inferred, not observed |
| `marts.fct_transactions`, `marts.fct_transactions_reporting` | Data Engineering / BI | Internal financial; includes payment metadata (`card_bin`) | Fact should not be exposed to an unrestricted audience; five reporting cards use published aggregates instead |
| `marts.daily_metric_rows` | Data Engineering / BI | Internal aggregated financial values and risk/province groups | Append-only publication payload; must be read through current-publication views |
| `marts.daily_metric_publications` | Data Engineering | Internal publication metadata | Date/run pointers; `FINAL` needed to determine current publication |
| `marts.daily_aggregate_checkpoints`, `marts.fact_checkpoints` | Data Engineering | Operational | Internal audit/processing offsets |
| `marts.agg_daily_sales` | Finance/BI (metric owner), Data Engineering (publisher) | Internal aggregate financial amounts | Read-only Metabase; filters out hard deletes |
| `marts.agg_daily_channel` | Finance/BI / Data Engineering | Internal performance metrics | Read-only Metabase; terminal denominator excludes PENDING |
| `marts.agg_daily_merchant` | Finance/BI / Data Engineering | Internal merchant-level financial aggregates | Read-only Metabase; smallest exposed grouping is merchant ID |
| `marts.agg_daily_fee_mcc_province` | Finance/BI / Data Engineering | Internal MCC/province fee aggregates | Read-only Metabase; province is historical as-of transaction creation |
| `marts.agg_daily_risk` | Finance/BI / Data Engineering | Internal historical risk aggregates | Read-only Metabase; risk is historical as-of transaction creation |

## Data treatment and caveats

- Source national identifier and mobile are classified as PII. The staging implementation masks them, retaining only the final four digits. Masking is **not** irreversible anonymization; do not treat those values as public.
- Raw Debezium JSON retains source before/after images for traceability and should be restricted, audited and subject to a retention policy before any real deployment.
- Transaction `card_bin` is the first six digits of a card number, not the full PAN, but remains payment metadata. Avoid exporting individual fact rows to dashboards without authorization.
- Analytics uses `Asia/Tehran` local Gregorian dates and months. Amounts/fees are integer Iranian rials. The simulated initial merchant snapshot cannot reproduce historical pre-CDC merchant risk/province and uses an explicitly flagged baseline assumption.
- CDC delivery and ClickHouse ReplacingMergeTree merges are **at least once / logically idempotent**, not end-to-end exactly once. Use `FINAL` or the published aggregate views for logical current state.
- Grant only SELECT on reporting views and their required underlying sources, never global SELECT on `raw`.

## Explicit sensitive columns in each reporting table (T5)

| Reporting relation | Metric owner / publisher | Sensitive columns (if any) |
|---|---|---|
| `marts.agg_daily_sales` | Finance/BI / Data Engineering | `success_count`, `success_amount` (internal financial aggregates) |
| `marts.agg_daily_channel` | Finance/BI / Data Engineering | `success_count`, `terminal_count` (internal payment performance) |
| `marts.agg_daily_merchant` | Finance/BI / Data Engineering | `merchant_id`, `success_count`, `success_amount` (merchant-level finance) |
| `marts.agg_daily_fee_mcc_province` | Finance/BI / Data Engineering | `fee_amount`, `success_count` (internal finance); `mcc`, `province` (business geography) |
| `marts.agg_daily_risk` | Finance/BI / Data Engineering | `risk_level`, `success_count`, `success_amount` (commercial risk and finance) |
| `marts.fct_transactions_reporting` | Finance/BI / Data Engineering | `transaction_id`, `merchant_id`, `amount`, `fee_amount`, `card_bin`, `risk_level_at_txn`, `province_at_txn` (transaction-level financial/payment metadata) |
| `marts.dim_merchant_history` | Finance/BI / Data Engineering | `merchant_id`, `name`, `province`, `risk_level` (merchant/business history) |

No raw national ID or phone number should be present in these reporting relations. The assignment allows masked or hashed treatment before reporting; masking is used here. Owners are assumed roles for the fictional exercise.
