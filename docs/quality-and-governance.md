# Data quality, privacy and ownership (T5)

This is the authoritative T5 explanation. For the complete owner/sensitive-column inventory, see [ownership-and-sensitivity.md](ownership-and-sensitivity.md). The fictional company roles in that document are design assumptions, **not** personnel assignments at Zibal.

## Automated quality checks

`transform/dbt/models/quality.yml` defines **11** accepted-value/key/relationship checks, all passed in the documented existing-environment acceptance run:

- Unique, non-null logical `transaction_id`; non-null `merchant_id`, `created_at` and merchant-history `merchant_id`.
- Merchant foreign-key relationship against the latest staged merchant entity.
- Accepted `status` (PENDING, SUCCESS, FAILED, REVERSED), `channel` (POS, IPG, QR), merchant/risk-at-transaction (LOW, MEDIUM, HIGH), and `is_deleted` (0/1).

Additional dbt singular tests in `transform/dbt/tests/` check SCD2 contiguous/valid windows, current-state equivalence, and valid transaction-time attribution. These are distinct from the 11-tagged final quality gates; they were run during development. Tests read the **logical `FINAL`-backed** view rather than count physical replay versions as duplicates.

## Reconciliation

`metabase/reconcile_daily.py` compares the PostgreSQL operational source and the published ClickHouse sales mart, **for every Tehran-local calendar date** in the supplied inclusive interval, with a failure on any missing date or difference in SUCCESS count/IRR amount.

Verified 2026-05-15 to 2026-11-13: **183/183 dates**, **4,483,082 SUCCESS**, **32,397,177,961,000 IRR**, with zero missing dates, mismatches, count difference or amount difference. Source-to-mart comparison assumes the simulator is paused and both CDC/transform streams are caught up. JSON evidence: `reports/m5b_daily_reconciliation.json`.

The five dashboard questions are compared independently by `metabase/benchmark_m4.py --check`: cards 1–3 strictly equal; cards 4–5 preserve grand totals while merchant province/risk may intentionally differ under transaction-time SCD2 attribution. The initial snapshot's pre-CDC historic merchant values are unknown, explicitly flagged as assumptions.

## Sensitive data and access

- Source `owner_national_id` and `owner_mobile` are **plaintext PII** within restricted PostgreSQL and `raw.cdc_merchants` before/after images. The `stg_merchants_events` and `stg_merchants_current` outputs retain only masked last-four-digit forms (not irreversible anonymization).
- `card_bin` is six-digit payment metadata, not a complete card number; transaction-level amounts and risk are internal financial data.
- The Metabase ClickHouse user `zibal_reporting` receives SELECT on five published views and necessary transitive sources, **not** `raw.cdc_*`. Those transitive grants include masked merchant event staging because ClickHouse 24.8 regular views use invoker privileges.
- This prototype uses local hard-coded demo accounts and passwords. Production use would require proper secret storage, rotation, audit logging, PII retention controls and stronger permission boundaries.
- There is no independently demonstrated automatic scan for plaintext PII in all future marts; review of projected SQL columns and read-only access grants was part of implementation. Treat a dedicated negative-access/PII test as an improvement, not an existing passing test.

## Failure handling and reproducibility

Checkpoint controllers advance only after their batch validates. Daily publication uses version pointers so a reader selects a complete committed day, not an unpublished candidate batch. A deliberately interrupted publish-before-checkpoint scenario was not separately tested. Periodic dbt cycles are manual or delegated to a **single** host scheduler; concurrency and multi-writer locks are not claimed.

The reviewer can run `scripts/verify.py`, which executes the current pipeline + 11 quality tests + health, strict daily reconciliation, and five-card comparison. See [operations](operations-and-recovery.md) for details.
