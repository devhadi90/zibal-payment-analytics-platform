#!/usr/bin/env python3
"""Portable one-command bootstrap for the Zibal T0-T5 Docker Compose demo.

Host requirements: Python 3.9+ and Docker Compose v2. No third-party Python packages.
This is the single maintained startup implementation (Windows/Linux/macOS).
"""

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib import error, request

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ("-f", "docker-compose.yml", "-f", "docker-compose.m2.yml",
           "-f", "docker-compose.m2b.yml", "-f", "docker-compose.m3a.yml")
CONNECT_API = "http://127.0.0.1:8083"
CONNECTOR_NAME = "zibal-postgres-cdc"
REQUIRED = (
    ".env.example", "source/postgres/seed.sql", "metabase/bootstrap.py",
    "metabase/optimized.py", "transform/dbt/run_staging_checkpointed.py",
    "transform/dbt/run_current_checkpointed.py", "transform/dbt/run_fact_checkpointed.py",
    "transform/dbt/run_daily_aggregates.py", "transform/dbt/run_pipeline_cycle.py",
    "transform/dbt/models/dim_merchant_history.sql",
    "transform/dbt/models/fct_transactions_reporting.sql",
    *(name for name in COMPOSE if name.endswith(".yml")),
)


def command(args, *, stdin=None, capture=False, confidential=False):
    """Run without a host shell (works with spaces in Windows/Unix paths)."""
    try:
        result = subprocess.run(
            list(args), cwd=ROOT, input=stdin, text=True, encoding="utf-8",
            errors="replace", stdout=subprocess.PIPE if capture or confidential else None,
            stderr=subprocess.PIPE if confidential else None, check=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("Docker CLI not found; install/start Docker Desktop or Docker Engine.") from exc
    except subprocess.CalledProcessError as exc:
        if confidential:
            # Never leak a SQL statement containing a password in an error log.
            raise RuntimeError("Sensitive database setup command failed; inspect database logs safely.") from None
        raise RuntimeError("Command failed (exit {}): {}".format(exc.returncode, " ".join(args))) from exc
    return (result.stdout or "").strip() if capture or confidential else ""


def compose(*args, stdin=None, capture=False, confidential=False):
    return command(("docker", "compose", *COMPOSE, *args), stdin=stdin,
                   capture=capture, confidential=confidential)


def last_line(output):
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("Expected command output but received none")
    return lines[-1]


def clickhouse(sql, *, multi=False, confidential=False):
    cli = 'clickhouse-client --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD"'
    if multi:
        cli += " --multiquery"
    return compose("exec", "-T", "clickhouse", "sh", "-c", cli,
                   stdin=sql + "\n", capture=not multi, confidential=confidential)


def pg_count(table):
    # Quoted inside the container shell, not the host shell.
    cmd = ('psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" '
           f'-tAc "SELECT count(*) FROM core.{table}"')
    return int(last_line(compose("exec", "-T", "postgres", "sh", "-c", cmd,
                                 capture=True)))


def running(service, *profiles):
    ids = compose(*profiles, "ps", "--status", "running", "--quiet", service,
                  capture=True)
    return bool(ids.strip())


def check_one_shot(service):
    ids = compose("ps", "-aq", service, capture=True).splitlines()
    if not ids:
        raise RuntimeError(f"Missing one-shot container: {service}")
    code = last_line(command(("docker", "wait", ids[-1]), capture=True))
    if code != "0":
        raise RuntimeError(f"One-shot service {service} failed (exit={code}); inspect its logs")
    print(f"M6_ONESHOT_OK {service}", flush=True)


def load_env():
    settings = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        settings[key.strip()] = value
    return settings


def wait_for_connect():
    for _ in range(90):
        try:
            with request.urlopen(CONNECT_API + "/connectors", timeout=5) as response:
                if response.status == 200:
                    return
        except (error.URLError, TimeoutError):
            pass
        time.sleep(2)
    raise RuntimeError("Kafka Connect REST API did not become ready")


def http_json(method, path, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = request.Request(CONNECT_API + path, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=60 if data else 15) as response:
            return response.status, json.load(response)
    except error.HTTPError as exc:
        # Do not print response bodies: Connect may echo credentials.
        return exc.code, None


def register_connector(settings):
    if not running("cdc-consumer", "--profile", "cdc"):
        raise RuntimeError("CDC consumer must be running before connector registration")
    if running("simulator"):
        raise RuntimeError("Stop simulator before the initial CDC snapshot")
    status, _ = http_json("GET", f"/connectors/{CONNECTOR_NAME}/status")
    if status == 200:
        print(f"M6_CONNECTOR_EXISTS {CONNECTOR_NAME}; no resnapshot", flush=True)
        return
    if status != 404:
        raise RuntimeError(f"Cannot inspect connector status (HTTP {status})")
    for key in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"):
        if not settings.get(key):
            raise RuntimeError(f"Missing {key} in .env")
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1", "database.hostname": "postgres", "database.port": "5432",
        "database.user": settings["POSTGRES_USER"],
        "database.password": settings["POSTGRES_PASSWORD"],
        "database.dbname": settings["POSTGRES_DB"],
        "topic.prefix": "zibal_cdc", "plugin.name": "pgoutput",
        "slot.name": "zibal_cdc_slot", "slot.drop.on.stop": "false",
        "publication.name": "zibal_cdc_publication",
        "publication.autocreate.mode": "filtered", "schema.include.list": "core",
        "table.include.list": "core.merchants,core.transactions",
        "snapshot.mode": "initial", "snapshot.fetch.size": "2048",
        "snapshot.max.threads": "1", "max.batch.size": "1024",
        "max.queue.size": "8192", "heartbeat.interval.ms": "10000",
        "tombstones.on.delete": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }
    print("M6_REGISTER_CONNECTOR starting source snapshot", flush=True)
    result, _ = http_json("POST", "/connectors", {"name": CONNECTOR_NAME, "config": config})
    if result == 409:
        result, _ = http_json("GET", f"/connectors/{CONNECTOR_NAME}/status")
    if result not in (200, 201):
        raise RuntimeError(f"Connector registration failed (HTTP {result}); inspect Connect logs")
    print(f"M6_CONNECTOR_OK {CONNECTOR_NAME}", flush=True)


def ensure_topics():
    print("M6_CREATE_CDC_TOPICS", flush=True)
    for topic in ("zibal_cdc.core.merchants", "zibal_cdc.core.transactions"):
        compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "kafka:9092", "--create", "--if-not-exists",
                "--topic", topic, "--partitions", "1", "--replication-factor", "1")


def await_snapshot(poll_seconds):
    table_exists = last_line(clickhouse(
        "SELECT count() FROM system.tables WHERE database = 'staging' AND name = 'staging_checkpoints'"))
    has_history = table_exists == "1" and last_line(clickhouse(
        "SELECT count() FROM staging.staging_checkpoints")) != "0"
    if has_history:
        print("M6_RESUME existing staging checkpoints; skipping fresh-snapshot assertion", flush=True)
        return
    expected_tx = pg_count("transactions")
    expected_merch = pg_count("merchants")
    print(f"M6_SOURCE_COUNTS transactions={expected_tx} merchants={expected_merch}", flush=True)
    for _ in range(240):
        raw = last_line(clickhouse("SELECT (SELECT count() FROM raw.cdc_transactions FINAL WHERE op = 'r'), "
                                   "(SELECT count() FROM raw.cdc_merchants FINAL WHERE op = 'r') FORMAT TSV"))
        values = raw.split()
        if len(values) != 2:
            raise RuntimeError("Unexpected raw snapshot count result")
        tx, merchants = map(int, values)
        print(f"M6_SNAPSHOT transactions={tx}/{expected_tx} merchants={merchants}/{expected_merch}", flush=True)
        if (tx, merchants) == (expected_tx, expected_merch):
            return
        if tx > expected_tx or merchants > expected_merch:
            raise RuntimeError("Snapshot rows exceed source counts; inspect CDC replay")
        time.sleep(poll_seconds)
    raise RuntimeError("Initial CDC snapshot timed out; inspect Connect/consumer logs")


def add_installments():
    print("M6_SCHEMA_EVOLUTION add nullable installments", flush=True)
    compose("exec", "-T", "postgres", "sh", "-c",
            'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
            stdin="ALTER TABLE core.transactions ADD COLUMN IF NOT EXISTS installments INTEGER;\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--add-installments-only", action="store_true",
                        help="Execute just the idempotent schema-evolution command on a running source")
    parser.add_argument("--skip-simulator", action="store_true",
                        help="Leave source unchanged for strict reconciliation")
    parser.add_argument("--snapshot-poll-seconds", type=int, default=15)
    options = parser.parse_args(argv)
    if options.snapshot_poll_seconds < 2:
        parser.error("--snapshot-poll-seconds must be >= 2")
    for name in REQUIRED:
        if not (ROOT / name).is_file():
            raise RuntimeError(f"Missing required project file: {name}")
    if not shutil.which("docker"):
        raise RuntimeError("Docker CLI not installed or missing from PATH")
    env_file = ROOT / ".env"
    if not env_file.exists():
        shutil.copyfile(ROOT / ".env.example", env_file)
        print("Created local .env from .env.example; do not commit it", flush=True)
    compose("config", "--quiet")
    if options.add_installments_only:
        add_installments()
        print("M6_SCHEMA_EVOLUTION_OK", flush=True)
        return
    if running("simulator"):
        raise RuntimeError("Simulator running. Stop it before rerunning startup: docker compose stop simulator")

    print("M6_START_SOURCE PostgreSQL + seed", flush=True)
    compose("up", "-d", "--build", "postgres", "seed")
    check_one_shot("seed")
    print("M6_START_INFRA ClickHouse, Metabase, Kafka, Connect", flush=True)
    compose("up", "-d", "--build", "clickhouse", "metabase", "kafka", "connect")
    print("M6_START_RAW_INIT", flush=True)
    compose("--profile", "cdc", "up", "-d", "--build", "raw-init")
    check_one_shot("raw-init")
    ensure_topics()  # Prevent UNKNOWN_TOPIC_OR_PART in the consumer on fresh Kafka volumes.
    compose("--profile", "cdc", "up", "-d", "--build", "cdc-consumer")

    reporting_password = load_env().get("CLICKHOUSE_REPORTING_PASSWORD", "zibal_reporting_dev_2026")
    if not reporting_password:
        raise RuntimeError("CLICKHOUSE_REPORTING_PASSWORD is empty")
    escaped = reporting_password.replace("\\", "\\\\").replace("'", "''")
    clickhouse(f"CREATE USER IF NOT EXISTS zibal_reporting IDENTIFIED WITH sha256_password BY '{escaped}';",
               multi=True, confidential=True)
    print("M6_METABASE_BASELINE", flush=True)
    compose("run", "--rm", "--no-deps", "metabase-bootstrap", "python", "bootstrap.py")

    wait_for_connect()
    register_connector(load_env())
    await_snapshot(options.snapshot_poll_seconds)
    add_installments()

    print("M6_BUILD_TRANSFORM", flush=True)
    compose("--profile", "transform", "build", "dbt-runner")
    compose("--profile", "transform", "run", "--rm", "--no-deps", "dbt-runner",
            "dbt", "run", "--select", "stg_transactions_events", "stg_merchants_events",
            "stg_transactions_current", "stg_merchants_current")
    for runner in ("run_staging_checkpointed.py", "run_current_checkpointed.py"):
        compose("--profile", "transform", "run", "--rm", "--no-deps", "dbt-runner",
                "python", runner, "--stream", "both", "--drain")
    compose("--profile", "transform", "run", "--rm", "--no-deps", "dbt-runner",
            "dbt", "run", "--select", "dim_merchant_history")
    compose("--profile", "transform", "run", "--rm", "--no-deps", "dbt-runner",
            "python", "run_fact_checkpointed.py", "--drain")
    compose("--profile", "transform", "run", "--rm", "--no-deps", "dbt-runner",
            "dbt", "run", "--select", "fct_transactions_reporting")
    compose("--profile", "transform", "run", "--rm", "--no-deps", "dbt-runner",
            "python", "run_daily_aggregates.py")
    compose("--profile", "transform", "run", "--rm", "--no-deps", "dbt-runner",
            "dbt", "run", "--select", "tag:reporting_aggregates")

    grants = "\n".join(f"GRANT SELECT ON {table} TO zibal_reporting;" for table in (
        "marts.agg_daily_sales", "marts.agg_daily_channel", "marts.agg_daily_merchant",
        "marts.agg_daily_fee_mcc_province", "marts.agg_daily_risk",
        "marts.dim_merchant_history", "marts.daily_metric_rows",
        "marts.daily_metric_publications", "staging.stg_merchants_events"))
    clickhouse(grants, multi=True)
    compose("run", "--rm", "--no-deps", "metabase-bootstrap", "python", "optimized.py", "--publish")
    compose("--profile", "transform", "run", "--rm", "--no-deps", "dbt-runner",
            "python", "run_pipeline_cycle.py", "--require-caught-up")

    if options.skip_simulator:
        print("M6_SIMULATOR_SKIPPED for quiescent verification", flush=True)
    else:
        print("M6_START_SIMULATOR (run an incremental cycle to refresh marts)", flush=True)
        compose("up", "-d", "simulator")
    print("M6_START_ALL_OK", flush=True)
    print("Metabase: http://localhost:3000 (baseline and optimized dashboards)", flush=True)
    print("Incremental cycle: make cycle (or run_pipeline_cycle.py via Compose)", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError) as exc:
        print(f"M6_START_ALL_FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
