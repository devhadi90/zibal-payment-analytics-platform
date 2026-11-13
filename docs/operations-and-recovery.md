# Runbook: deployment, incremental execution and recovery

This is the authoritative operational reference for T0–T2/T5, superseding early M1/M2/M3 milestone runbooks. Commands below assume **Python 3.9+**, Docker Engine/Desktop with Compose v2, and the project root; all services are local and use demo credentials.

## Start (one command)

```bash
python3 scripts/start.py
```

On Windows, run `py -3 scripts/start.py`. On Ubuntu/macOS, run `python3 scripts/start.py`. No PowerShell or extra host Python packages are needed.

The script creates `.env` only if missing, seeds 2,000 merchants + 5,000,000 transactions (180-day history), starts ClickHouse, Kafka/Connect and Metabase, initializes raw tables, explicitly creates the two CDC Kafka topics, runs the consumer, registers Debezium, waits for the initial snapshot, adds `installments`, creates dbt layers in dependency order, publishes the five aggregate views, grants the read-only Metabase account and installs both dashboards. Only then does it start the ~20-TPS simulator. The simulator should be **stopped before rerunning the full startup or strict acceptance checks**. After earlier fixes, the portable Python startup completed a separate, uninterrupted **fresh-volume** run with the simulator enabled (`M6_START_ALL_OK`, process exit 0). The simulator was confirmed started, but sustained post-start transaction rate was not established by that startup log alone.

```bash
docker compose stop simulator
python3 scripts/start.py --skip-simulator
python3 scripts/verify.py
```

Use `py -3` instead of `python3` on Windows. Verification optionally takes `--start YYYY-MM-DD --end YYYY-MM-DD`.

A GNU Make wrapper is optional: `make up`, `make test`, `make cycle`. The Python scripts are canonical, including when Make is unavailable. The four Compose definitions are *one composed project*; see [architecture](architecture-and-data-model.md).

## Incremental operation

The Debezium connector and Kafka-to-ClickHouse consumer are long-running. Once a source transaction or merchant changes, the **dbt transformations are not continuously scheduled**. Run the existing single-writer cycle manually, or arrange a single external host schedule:

```bash
docker compose -f docker-compose.yml -f docker-compose.m2.yml -f docker-compose.m2b.yml -f docker-compose.m3a.yml --profile transform run --rm --no-deps dbt-runner python run_pipeline_cycle.py
```

Order: raw → `stg_*_events` (bounded topic offset) → `stg_*_current` → transaction facts → reporting fact view/SCD2 → dirty-day aggregate publication → 11 quality tests → health. Actual execution may skip up-to-date steps; controllers commit their own checkpoints only after validation. A physical `ReplacingMergeTree` may contain replay duplicates; use logical `FINAL` or reporting views.

## Recovery and health

- **Don't reset volumes or CDC offsets** to fix a stalled job. Check `docker compose ps -a`, `docker compose logs connect cdc-consumer clickhouse`, Kafka Connect status at `http://localhost:8083/connectors/zibal-postgres-cdc/status`, disk space, source WAL slot and per-stream checkpoints.
- The connector registration is idempotent on an existing deployment. Topics are created before the consumer to avoid `UNKNOWN_TOPIC_OR_PART`. A running Connect container is not necessarily a ready API; its readiness must be checked.
- Restart a failed transform cycle. Bounded replay and `ReplacingMergeTree`/`FINAL` support logical idempotency; the code assumes **one writer**. There is no distributed lock.
- Daily aggregation writes all candidate groups, validates them, **then** publishes the date/run pointer. Unreferenced rows from a failed attempt may remain but aren't visible to the five published aggregate views. Crash-injection between pointer publication and checkpoint advance has not been separately demonstrated.
- Source/consumer end-to-end completeness is not established just by raw-to-staging offset parity: compare PostgreSQL source, connector status/LSN, Kafka offsets and the final day-by-day reconciliation when stronger assurance is needed.
- PostgreSQL uses a replication slot; extended Connect downtime can accumulate WAL. `max_slot_wal_keep_size` is bounded in the demo, so a slot may become invalid during excessive downtime. Recover by first assessing slot state and a controlled CDC resnapshot/reseed plan, **never silently reset offsets**.
- Stop the simulator and drain CDC before strict source/mart reconciliation. The default dated evidence is Nov 2026; on a fresh later seed choose its published date range for verification. Acceptance script accepts `--start` and `--end` dates.

## Useful commands

```bash
# View running services (stop simulator before strict test)
docker compose -f docker-compose.yml -f docker-compose.m2.yml -f docker-compose.m2b.yml -f docker-compose.m3a.yml --profile cdc --profile transform ps -a

# Health: downstream checkpoint parity + Debezium task state
docker compose -f docker-compose.yml -f docker-compose.m2.yml -f docker-compose.m2b.yml -f docker-compose.m3a.yml --profile transform run --rm --no-deps dbt-runner python check_pipeline_health.py --require-caught-up

# Schema evolution demonstration: idempotent ALTER TABLE after startup
python3 scripts/start.py --add-installments-only
```

Do not use `docker compose down -v`, `docker volume prune`, or `docker system prune` on a populated project. Fixed localhost port bindings are meant for a **single active Compose project**, not simultaneous copies.

## Resource and security limits

An isolated fresh start on Docker Desktop recorded **943 per-container readings across 144 polling rounds** (2026-11-13 23:49–2026-11-14 00:06, UTC+03:30); peak **concurrent sum** was **5,283 MiB (5.16 GiB)**. This measures container usage, **not** the operating system, Docker VM overhead, or total physical-host memory; a true 16 GB machine and Ubuntu/macOS (especially Apple Silicon) remain untested. PostgreSQL/ClickHouse and Kafka use local named Docker volumes; Metabase uses an app database on PostgreSQL. Passwords in `.env.example` are demo values; do not expose these ports or credentials on an internet-connected host. No high-availability Kafka, orchestration service, credential rotation or organization-wide governance was implemented, in line with scope.
