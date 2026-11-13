# Verification evidence and Metabase performance (T0–T5)

This is the authoritative validation narrative. Raw machine-generated results remain in `reports/` with original script-generated filenames; see `reports/README.md`. Results below are **observations**, not estimates or future guarantees.

## Verified delivery stages

| Requirement | Evidence | Limit |
|---|---|---|
| T0 source + simulator | 2,000 merchants and ≥5M payments; live status/merchant changes and hard deletes exercised; direct PostgreSQL baseline dashboard | Latest clean startup launched the simulator; sustained TPS after startup was not measured in that run |
| T1 initial snapshot + incremental CDC | Debezium merchant/transaction snapshot; c/u/d; replay after consumer downtime; `installments` schema change | Single Kafka partition per topic, no HA |
| T2 staging/current | Bounded offset checkpoints, logical `FINAL`, separate current-state tombstones; masked PII | Single writer, no continuous dbt scheduler |
| T3 historical merchant attributes | Half-open SCD2 window tests; controlled merchant1 change HIGH/Khorasan Razavi → LOW/Tehran; new transaction dynamically reattributed, older transactions kept old values; one dirty date republished | Pre-snapshot state is assumed, not observed |
| T4 dashboard migration | Five optimized Metabase questions; comparison and 20-run benchmark | API latency not browser rendering |
| T5 tests and reconciliation | 11/11 tagged dbt tests; daily source/mart count & IRR amount match across 183 days | Fixed quiet reference interval |
| Reproducible deployment | Earlier startup issues fixed; latest `python scripts/start.py` finished in one uninterrupted run on empty named volumes with simulator enabled (`M6_START_ALL_OK`, exit 0). Restored original environment subsequently passed `M5D_ACCEPTANCE_OK` (exit 0) | Clean run performed on Windows/Docker Desktop with existing cached images; not yet executed on Linux/macOS or a physical 16 GB host |

## Benchmark design

Identical `2026-11-13` Tehran-local as-of date; fixed query SQL from the saved baseline and optimized cards; Metabase `POST /api/dataset` API wall-clock timings. One warm-up and **20 measured runs** per card **per database**, with interleaved execution order and cache state checked. PostgreSQL had ordinary indexes; no deliberate source slowdown. Compare medians and p95 with original samples in `reports/m4b_benchmark.json`.

| Card | PostgreSQL median ms | ClickHouse median ms | Median speedup | Concrete optimization |
|---|---:|---:|---:|---|
| 1. Daily SUCCESS count & amount (90d) | 1,397.74 | 25.11 | 55.68× | Date-grain precomputed SUCCESS aggregates |
| 2. Terminal success rate by channel (30d) | 1,098.70 | 22.73 | 48.34× | Precomputed `(date,channel)` SUCCESS and terminal counts |
| 3. Top 10 merchants, current month | 425.31 | 29.02 | 14.66× | Merchant/day SUCCESS amounts, then monthly group and top 10 |
| 4. Fees by MCC/province (90d) | 962.18 | 24.43 | 39.39× | Precomputed historical `(date,MCC,province)` fee totals |
| 5. Monthly SUCCESS by risk | 3,262.60 | 20.43 | 159.74× | Precomputed risk **as of transaction time** and calendar-month group |

**Not a browser dashboard timing benchmark** and not a proof of improvement at different concurrency/hardware. The `baseline_first_run.json`/`baseline_benchmark.json` reports are early/reference results and are not the final comparative measurement.

## Result equality policy and historical differences

- **Cards 1–3:** exact row-key/value match (90 rows, 90 rows, 10 rows respectively; zero mismatched cells).
- **Card 4:** PostgreSQL baseline groups by merchant's *current* province while ClickHouse uses province **at transaction creation**. Grand fees reconcile; **94 differing dimensional metric cells** at the fixed date. An example of why: merchant 1 changed from `Khorasan Razavi` to `Tehran` on 2026-11-13; older transactions retain the prior attributed province, while newer ones follow Tehran. This test proves dynamic attribution mechanics, not a reconstruction of merchant history before initial snapshot.
- **Card 5:** PostgreSQL groups monthly successes by *current* risk; ClickHouse uses **risk at transaction creation**. Grand count and amount reconcile while **42 dimensional metric cells** differ.

`reports/m4b_comparison.json` contains all comparison rules and differences. Do not assert that cards 4–5 must have identical dimensional rows: the PDF allows explained differences for cards 1–4 and specifically requires historical risk for card 5.

## Strict day-by-day reconciliation

For 2026-05-15 through 2026-11-13 inclusive: **183 published days**, PostgreSQL and ClickHouse each reported **4,483,082** successful non-deleted transactions and **32,397,177,961,000 IRR**. Missing dates = 0, mismatched dates = 0, count delta = 0, amount delta = 0. Report: `reports/m5b_daily_reconciliation.json`.

## Reproducibility and limitations

The earlier fresh-volume attempt required iterative fixes (Metabase image availability, CDC topics before consumer, source-count parsing), and was run with the simulator disabled. **A later independent fresh-volume run of the corrected portable entry point** (`py -3 scripts/start.py`, 2026-11-13/14, Windows + Docker Desktop) completed **without manual intervention during that run**, with `START_EXIT_CODE=0` and `M6_START_ALL_OK`. It created 5,000,000 source transactions and 2,000 merchants, captured the complete initial snapshot, applied `installments`, completed dbt processing, published reporting data, passed 11 dbt tests, reported zero downstream CDC backlog, and started the simulator. Container startup is confirmed; sustained simulator rate after startup is not inferred from these logs. The restoration of the original environment subsequently passed Python acceptance with `M5D_ACCEPTANCE_OK`, 11/11 tests, 183/183 reconciled dates, five card comparisons and zero reporting backlog.

### Startup memory observation

During that fresh run, `docker stats --no-stream` was polled in **144 rounds**, recording **943 individual container readings** from **2026-11-13 23:49:41** to **2026-11-14 00:06:26** (UTC+03:30). The highest sum of memory reported for containers present in the same polling round was **5,283 MiB = 5.16 GiB**, observed at **2026-11-14 00:05:28**. Individually observed peaks were ClickHouse **1.29 GiB**, Metabase **1.18 GiB**, Debezium Connect **1.12 GiB**, PostgreSQL **0.81 GiB**, Kafka **0.80 GiB**, and CDC consumer **0.13 GiB**; individual peaks were not necessarily simultaneous. The Docker Engine memory allocation was **15.5 GiB** on a **31.8 GiB Windows host**.

**Measurement limits:** This is sampled **container-reported memory**, not the full physical-machine high-water mark. It excludes Windows, Docker Desktop/VM overhead and other host processes, and may miss spikes between samples; it is strong supporting evidence for the 16 GB target but does **not** prove installation on an actual 16 GB computer. Startup images were already cached. Linux/macOS (including Apple Silicon) remain unverified. The raw `fresh-start.log` and `fresh-start-memory.log` were retained outside the Git submission; the findings above are a compact reproducible evidence summary, not a claim of attached raw logs.

The code does not automatically schedule ongoing dbt transforms, enforce multi-writer locks, or claim HA/production security. Improvements beyond the PDF are not treated as delivered requirements.
