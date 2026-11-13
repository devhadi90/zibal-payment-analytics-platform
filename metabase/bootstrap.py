"""Idempotent baseline Metabase setup for the Zibal M1 interview exercise.

Uses only Python stdlib and Metabase's HTTP API.  On first run it creates an
admin, the Postgres source and ClickHouse test connections, five saved cards,
and a dashboard; on subsequent runs it reuses them.  It also collects observed
card API request durations.  No timing is described as verified until run.
"""
import argparse
import json
import os
from pathlib import Path
import re
import statistics
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

URL = os.getenv("MB_URL", "http://metabase:3000").rstrip("/")
EMAIL = os.environ["MB_ADMIN_EMAIL"]
PASSWORD = os.environ["MB_ADMIN_PASSWORD"]
SOURCE_PASSWORD = os.environ["MB_SOURCE_PASSWORD"]
CLICKHOUSE_REPORTING_PASSWORD = os.environ["MB_CH_REPORTING_PASSWORD"]
SOURCE_DB = os.getenv("POSTGRES_DB", "zibal")
AS_OF_DATE = os.getenv("AS_OF_DATE", "").strip()
CARDS_PATH = Path(__file__).parent / "cards" / "baseline.sql"
REPORTS = Path("/reports")


class MetabaseError(RuntimeError):
    pass


def request(path, method="GET", data=None, session=None, timeout=180):
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    if session:
        headers["X-Metabase-Session"] = session
    payload = json.dumps(data).encode("utf-8") if data is not None else None
    req = Request(URL + path, data=payload, method=method, headers=headers)
    try:
        with urlopen(req, timeout=timeout) as response:
            content = response.read()
            return json.loads(content) if content else {}
    except HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:1200]
        raise MetabaseError(f"{method} {path}: HTTP {e.code}: {body}") from e
    except URLError as e:
        raise MetabaseError(f"{method} {path}: {e}") from e


def wait_healthy(attempts=180, interval=4):
    for _ in range(attempts):
        try:
            data = request("/api/health", timeout=5)
            if data.get("status") == "ok":
                return
        except (MetabaseError, TimeoutError, ValueError):
            pass
        time.sleep(interval)
    raise MetabaseError("Metabase never became healthy; inspect docker compose logs metabase")


def login_or_setup():
    props = request("/api/session/properties")
    if props.get("has-user-setup"):
        result = request("/api/session", "POST", {"username": EMAIL, "password": PASSWORD})
    else:
        token = props.get("setup-token")
        if not token:
            raise MetabaseError("No setup token and no admin configured")
        result = request("/api/setup", "POST", {
            "token": token,
            "user": {"first_name": "Zibal", "last_name": "Demo",
                     "email": EMAIL, "password": PASSWORD},
            "prefs": {"site_name": "Zibal Data Platform M1", "site_locale": "en"}
        })
    sid = result.get("id")
    if not sid:
        raise MetabaseError("Login/setup produced no Metabase session ID")
    return sid


def collection(result):
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        for key in ("data", "items", "results"):
            if isinstance(result.get(key), list):
                return result[key]
    raise MetabaseError("Unexpected paginated API response format")


def ensure_database(session, name, engine, details):
    for db in collection(request("/api/database", session=session)):
        if db.get("name") == name:
            if db.get("engine") != engine:
                raise MetabaseError(f"Existing database {name!r} has engine {db.get('engine')!r}")
            return db["id"]
    result = request("/api/database", "POST", {
        "name": name, "engine": engine, "details": details,
        "is_full_sync": False, "is_on_demand": False
    }, session=session)
    print(f"created Metabase connection: {name} ({engine})", flush=True)
    return result["id"]


def validate_database_query(session, db_id, label):
    """Exercise the actual driver, not just the saved connection metadata."""
    result = request("/api/dataset", "POST", {
        "database": db_id, "type": "native", "native": {"query": "SELECT 1 AS connection_check"}
    }, session=session)
    if result.get("error") or result.get("status") == "failed" or not result.get("data", {}).get("rows"):
        raise MetabaseError(f"Database connectivity test failed for {label}: {result.get('error')}")
    print(f"PASS: Metabase executed SELECT 1 through {label}", flush=True)


def parse_cards(sql_file):
    text = Path(sql_file).read_text(encoding="utf-8")
    chunks = re.split(r"(?m)^-- name: ([a-z0-9_]+)[^\n]*$", text)
    entries = []
    for i in range(1, len(chunks), 2):
        name = chunks[i]
        # Next comment card headers appear in the next delimiter; trailing whitespace removed.
        sql = chunks[i+1].strip()
        if not sql or not sql.upper().startswith("SELECT"):
            raise ValueError(f"Card {name} does not begin with a SELECT")
        entries.append((name, sql))
    if len(entries) != 5 or len({name for name, _ in entries}) != 5:
        raise ValueError(f"Expected 5 uniquely named cards, found {len(entries)}")
    return entries


def compile_sql(sql):
    if AS_OF_DATE:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", AS_OF_DATE):
            raise ValueError("AS_OF_DATE must be YYYY-MM-DD")
        replacement = f"DATE '{AS_OF_DATE}'"
    else:
        replacement = "(now() AT TIME ZONE 'Asia/Tehran')::date"
    sql = sql.replace("{{AS_OF}}", replacement)
    if "{{AS_OF}}" in sql or "{{" in sql:
        raise ValueError("An unresolved template expression remains in the SQL")
    return sql


def existing_cards(session):
    return {card["name"]: card for card in collection(request("/api/card", session=session))}


def ensure_card(session, db_id, name, sql, display):
    label = f"M1 Baseline — {name}"
    dataset = {"database": db_id, "type": "native", "native": {"query": sql}}
    cards = existing_cards(session)
    if label in cards:
        card = cards[label]
        # Reuse existing saved card IDs, update SQL if a fixed as_of_date was configured.
        if card.get("dataset_query") != dataset:
            request(f"/api/card/{card['id']}", "PUT", {
                "dataset_query": dataset, "display": display
            }, session=session)
        return card["id"]
    result = request("/api/card", "POST", {
        "name": label, "display": display,
        "dataset_query": dataset, "visualization_settings": {},
        "description": "M1 baseline: reads PostgreSQL core, before optimization."
    }, session=session)
    return result["id"]


def ensure_dashboard(session, card_ids):
    label = "Daily sales — PostgreSQL baseline (M1)"
    dashboards = collection(request("/api/dashboard", session=session))
    found = next((d for d in dashboards if d.get("name") == label), None)
    if found:
        dash_id = found["id"]
    else:
        dash_id = request("/api/dashboard", "POST", {
            "name": label, "description": "Baseline cards read core; marts cards arrive in M4."
        }, session=session)["id"]
    # Reusing a dashboard? Avoid recreating cards each time, and check membership.
    current = request(f"/api/dashboard/{dash_id}", session=session)
    placed = {c.get("card_id") for c in current.get("dashcards", [])}
    if not set(card_ids).issubset(placed):
        retained = [
            {"id": c["id"], "card_id": c["card_id"],
             "row": c["row"], "col": c["col"],
             "size_x": c["size_x"], "size_y": c["size_y"]}
            for c in current.get("dashcards", []) if c.get("card_id") is not None
        ]
        new_cards = [
            {"id": -idx, "card_id": card_id,
             "row": (idx - 1)*5, "col": 0,
             "size_x": 12, "size_y": 5}
            for idx, card_id in enumerate(card_ids, start=1)
            if card_id not in placed
        ]
        request(f"/api/dashboard/{dash_id}", "PUT", {
            "dashcards": retained + new_cards
        }, session=session)
    return dash_id


def query_card(session, card_id):
    start = time.perf_counter()
    data = request(f"/api/card/{card_id}/query", "POST", {"parameters": []},
                   session=session, timeout=180)
    elapsed = time.perf_counter()-start
    if data.get("status") == "failed" or data.get("error"):
        raise MetabaseError(f"Card {card_id} failed: {data.get('error')}")
    return round(elapsed*1000, 2), len(data.get("data", {}).get("rows", []))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--bench", action="store_true")
    ap.add_argument("--runs", type=int, default=20)
    args = ap.parse_args()
    wait_healthy()
    session = login_or_setup()
    if args.verify:
        dbs = {d["name"]: d for d in collection(request("/api/database", session=session))}
        names = {f"M1 Baseline — {name}" for name, _ in parse_cards(CARDS_PATH)}
        actual = existing_cards(session)
        dashboards = collection(request("/api/dashboard", session=session))
        if not {"Zibal source PostgreSQL", "Zibal analytical ClickHouse"}.issubset(dbs):
            raise MetabaseError("Missing one or both M1 Metabase connections")
        if not names.issubset(actual):
            raise MetabaseError("Missing some of the five baseline questions")
        if not any(d.get("name") == "Daily sales — PostgreSQL baseline (M1)" for d in dashboards):
            raise MetabaseError("Missing Daily sales dashboard")
        validate_database_query(session, dbs["Zibal source PostgreSQL"]["id"], "PostgreSQL")
        validate_database_query(session, dbs["Zibal analytical ClickHouse"]["id"], "ClickHouse")
        print("PASS: 2 live Metabase connections, 5 baseline questions, dashboard", flush=True)
        return

    pg_id = ensure_database(session, "Zibal source PostgreSQL", "postgres", {
        "host": "postgres", "port": 5432, "dbname": SOURCE_DB,
        "user": "metabase_ro", "password": SOURCE_PASSWORD, "ssl": False
    })
    click_id = ensure_database(session, "Zibal analytical ClickHouse", "clickhouse", {
        "host": "clickhouse", "port": 8123, "dbname": "default",
        "user": "zibal_reporting", "password": CLICKHOUSE_REPORTING_PASSWORD, "ssl": False
    })
    validate_database_query(session, pg_id, "PostgreSQL")
    validate_database_query(session, click_id, "ClickHouse")
    ids = []
    for i, (name, sql) in enumerate(parse_cards(CARDS_PATH)):
        display = ["line", "line", "table", "bar", "bar"][i]
        ids.append(ensure_card(session, pg_id, name, compile_sql(sql), display))
    dash_id = ensure_dashboard(session, ids)
    print(f"PASS: source dashboard created: /dashboard/{dash_id}", flush=True)

    repeats = args.runs if args.bench else 1
    if repeats < 1:
        raise ValueError("--runs must be >= 1")
    records = []
    for card_id, (name, _) in zip(ids, parse_cards(CARDS_PATH)):
        times = []
        for iteration in range(repeats):
            ms, rows = query_card(session, card_id)
            times.append(ms)
            print(f"{name} run {iteration+1}/{repeats}: {ms} ms, result_rows={rows}", flush=True)
        sorted_times = sorted(times)
        p95 = sorted_times[max(0, int(.95*len(sorted_times)+.9999)-1)]
        records.append({"card": name, "card_id": card_id, "samples_ms": times,
                        "median_ms": round(statistics.median(times), 2), "p95_ms": p95,
                        "last_result_rows": rows})
    REPORTS.mkdir(parents=True, exist_ok=True)
    name = "baseline_benchmark.json" if args.bench else "baseline_first_run.json"
    output = REPORTS / name
    output.write_text(json.dumps({
        "measured_utc": datetime.now(timezone.utc).isoformat(),
        "as_of_date": AS_OF_DATE or "dynamic Asia/Tehran current date",
        "via": "Metabase API, end-to-end request time (not SQL-only execution)",
        "repetitions": repeats, "cards": records
    }, indent=2), encoding="utf-8")
    print(f"Wrote measured card runtimes: {output}", flush=True)


if __name__ == "__main__":
    main()
