#!/usr/bin/env python3
"""M4-A: preflight + idempotent five-card ClickHouse Metabase dashboard.

Uses the installed M1 `bootstrap.py` HTTP client, credentials, and read-only
Metabase analytical connection. Baseline cards/dashboard are never modified.
Run `--probe` before `--publish`. No third-party Python packages required.
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import re
import sys

import bootstrap as mb

CARD_FILE = Path(__file__).resolve().parent / "cards" / "optimized.sql"
DB_NAME = "Zibal analytical ClickHouse"
DASHBOARD_NAME = "Daily sales — ClickHouse optimized (M4)"
DISPLAY = ["line", "line", "table", "bar", "bar"]


def cards():
    content = CARD_FILE.read_text(encoding="utf-8")
    chunks = re.split(r"(?m)^-- name: ([a-z0-9_]+)[^\n]*$", content)
    result = [(chunks[i], chunks[i + 1].strip()) for i in range(1, len(chunks), 2)]
    if len(result) != 5 or len({name for name, _ in result}) != 5:
        raise ValueError(f"Expected exactly five unique SQL queries; found {len(result)}")
    for name, query in result:
        if not query.startswith("SELECT"):
            raise ValueError(f"Optimized SQL {name} must start with SELECT")
        if "core." in query or "raw." in query:
            raise ValueError(f"Optimized query {name} reads a source/raw database")
    return result


def compile_query(sql: str, as_of: str) -> str:
    if as_of:
        date.fromisoformat(as_of)
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", as_of):
            raise ValueError("--as-of must be exactly YYYY-MM-DD")
        local_day = f"toDate('{as_of}')"
    else:
        local_day = "toDate(now('Asia/Tehran'))"
    compiled = sql.replace("{{AS_OF}}", local_day)
    if "{{" in compiled or "}}" in compiled:
        raise ValueError("Unresolved template marker")
    return compiled


def query(mb_session, database_id: int, sql: str):
    result = mb.request("/api/dataset", "POST", {
        "database": database_id,
        "type": "native",
        "native": {"query": sql}
    }, session=mb_session, timeout=180)
    if result.get("status") == "failed" or result.get("error"):
        raise RuntimeError(f"Metabase analytical query failed: {result.get('error')}")
    rows = result.get("data", {}).get("rows")
    if rows is None:
        raise RuntimeError("Metabase returned no data.rows; cannot confirm execution")
    return len(rows)


def get_analytical_database(session):
    dbs = {db["name"]: db for db in mb.collection(mb.request("/api/database", session=session))}
    if DB_NAME not in dbs:
        raise RuntimeError(f"Missing existing analytical Metabase connection: {DB_NAME}")
    db = dbs[DB_NAME]
    if db.get("engine") != "clickhouse":
        raise RuntimeError(f"Expected ClickHouse; got {db.get('engine')!r}")
    return db["id"]


def ensure_cards(session, db_id: int, parsed, as_of: str):
    known = mb.existing_cards(session)
    ids = []
    for index, (name, sql) in enumerate(parsed):
        label = f"M4 Optimized — {name}"
        dataset = {
            "database": db_id,
            "type": "native",
            "native": {"query": compile_query(sql, as_of)}
        }
        if label in known:
            card = known[label]
            if card.get("dataset_query") != dataset:
                mb.request(f"/api/card/{card['id']}", "PUT", {
                    "dataset_query": dataset, "display": DISPLAY[index]
                }, session=session)
            card_id = card["id"]
        else:
            created = mb.request("/api/card", "POST", {
                "name": label,
                "display": DISPLAY[index],
                "dataset_query": dataset,
                "visualization_settings": {},
                "description": "M4 optimized: published ClickHouse marts, historical risk/province."
            }, session=session)
            card_id = created["id"]
        ids.append(card_id)
        print(f"card {index + 1}/5: id={card_id} {name}", flush=True)
    return ids


def ensure_dashboard(session, card_ids):
    found = next((d for d in mb.collection(mb.request("/api/dashboard", session=session))
                  if d.get("name") == DASHBOARD_NAME), None)
    if found:
        dashboard_id = found["id"]
    else:
        dashboard_id = mb.request("/api/dashboard", "POST", {
            "name": DASHBOARD_NAME,
            "description": "M4 ClickHouse dashboard. PostgreSQL baseline is deliberately preserved."
        }, session=session)["id"]
    detail = mb.request(f"/api/dashboard/{dashboard_id}", session=session)
    placed = {item.get("card_id") for item in detail.get("dashcards", [])}
    new = []
    for position, card_id in enumerate(card_ids, 1):
        if card_id not in placed:
            new.append({"id": -position, "card_id": card_id,
                        "row": (position - 1) * 5, "col": 0,
                        "size_x": 12, "size_y": 5})
    if new:
        retained = [
            {"id": item["id"], "card_id": item["card_id"],
             "row": item["row"], "col": item["col"],
             "size_x": item["size_x"], "size_y": item["size_y"]}
            for item in detail.get("dashcards", []) if item.get("card_id") is not None
        ]
        mb.request(f"/api/dashboard/{dashboard_id}", "PUT", {
            "dashcards": retained + new
        }, session=session)
    return dashboard_id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--probe", action="store_true", help="Read-only: execute all five SQL queries via Metabase")
    mode.add_argument("--publish", action="store_true", help="Create/update optimized cards and dashboard")
    parser.add_argument("--as-of", default="", help="YYYY-MM-DD fixed Tehran reporting day (default: dynamic)")
    args = parser.parse_args()
    parsed = cards()
    mb.wait_healthy()
    session = mb.login_or_setup()
    db_id = get_analytical_database(session)
    print(f"Connected to Metabase analytical database id={db_id}", flush=True)
    for idx, (name, query_text) in enumerate(parsed, 1):
        n = query(session, db_id, compile_query(query_text, args.as_of))
        print(f"PASS {idx}/5: {name}, rows={n}", flush=True)
    if args.probe:
        print("M4A_PROBE_OK", flush=True)
        return
    ids = ensure_cards(session, db_id, parsed, args.as_of)
    dashboard_id = ensure_dashboard(session, ids)
    print(f"M4A_DASHBOARD_OK dashboard_id={dashboard_id} url=http://localhost:3000/dashboard/{dashboard_id}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"M4A_FAILED: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
