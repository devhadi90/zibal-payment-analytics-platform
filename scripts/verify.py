#!/usr/bin/env python3
"""Portable strict T0-T5 acceptance gates for a quiescent Zibal deployment."""

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ("docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.m2.yml",
           "-f", "docker-compose.m2b.yml", "-f", "docker-compose.m3a.yml")


def compose(*args, input_text=None, capture=False):
    try:
        result = subprocess.run((*COMPOSE, *args), cwd=ROOT,
                                input=input_text, text=True, encoding="utf-8",
                                errors="replace", stdout=subprocess.PIPE if capture else None,
                                check=True)
        return (result.stdout or "").strip()
    except FileNotFoundError as exc:
        raise RuntimeError("Docker CLI not found") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Compose command failed (exit {exc.returncode})") from exc


def published_window():
    query = "SELECT count(), min(date), max(date) FROM marts.daily_metric_publications FINAL FORMAT TSV\n"
    result = compose("exec", "-T", "clickhouse", "sh", "-c",
                     'clickhouse-client --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD"',
                     input_text=query, capture=True)
    lines = [line.strip() for line in result.splitlines() if line.strip()]
    fields = lines[-1].split("\t") if lines else []
    if len(fields) != 3 or int(fields[0]) < 1:
        raise RuntimeError("No valid published reporting date range")
    return fields[1], fields[2]


def validate_date(value):
    try:
        if dt.date.fromisoformat(value).isoformat() != value:
            raise ValueError(value)
    except ValueError as exc:
        raise RuntimeError(f"Expected yyyy-MM-dd date, got {value!r}") from exc
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    options = parser.parse_args(argv)
    for name in ("docker-compose.yml", "docker-compose.m2.yml", "docker-compose.m2b.yml",
                 "docker-compose.m3a.yml", "transform/dbt/run_pipeline_cycle.py",
                 "metabase/reconcile_daily.py", "metabase/benchmark_m4.py"):
        if not (ROOT / name).is_file():
            raise RuntimeError(f"Missing required file: {name}")
    print("M5D_GATE Compose configuration", flush=True)
    compose("config", "--quiet")
    if compose("ps", "--quiet", "simulator", capture=True):
        raise RuntimeError("Simulator is running. Stop it and let CDC catch up before strict reconciliation")

    start, end = options.start, options.end
    if not start or not end:
        actual_start, actual_end = published_window()
        start = start or actual_start
        end = end or actual_end
    validate_date(start)
    validate_date(end)
    if start > end:
        raise RuntimeError(f"Start date {start} must not exceed end date {end}")
    print(f"M5D_WINDOW start={start} end={end}", flush=True)

    checks = (
        ("checkpointed pipeline + 11 dbt tests + health",
         ("--profile", "transform", "run", "--rm", "--no-deps", "dbt-runner",
          "python", "run_pipeline_cycle.py", "--require-caught-up")),
        ("date-scoped source-to-mart strict reconciliation",
         ("run", "--rm", "--no-deps", "metabase-bootstrap", "python", "reconcile_daily.py",
          "--start", start, "--end", end)),
        ("five dashboard data comparisons",
         ("run", "--rm", "--no-deps", "metabase-bootstrap", "python", "benchmark_m4.py",
          "--check", "--as-of", end)),
    )
    for label, arguments in checks:
        print(f"M5D_GATE_START {label}", flush=True)
        compose(*arguments)
        print(f"M5D_GATE_OK {label}", flush=True)
    print(f"M5D_ACCEPTANCE_OK start={start} end={end}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError) as exc:
        print(f"M5D_ACCEPTANCE_FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
