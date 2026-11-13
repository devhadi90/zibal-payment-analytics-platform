"""Small at-least-once Kafka -> ClickHouse raw CDC consumer.

Semantics: consume Debezium JSON envelope, insert a batch, *then* commit Kafka
positions synchronously. A crash after insert but before commit replays records;
read raw with FINAL to deduplicate (topic, partition, offset). Never assume
physical row count is deduplicated. PII intentionally stays ONLY in raw JSON.
"""
import argparse
import base64
import json
import os
import signal
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

TOPIC_TO_TABLE = {
    "zibal_cdc.core.merchants": "raw.cdc_merchants",
    "zibal_cdc.core.transactions": "raw.cdc_transactions",
}
STOP = False


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")) if value is not None else ""


def optional_number(value):
    return int(value) if value is not None else None


def decode_event(topic, partition, offset, kafka_ts_ms, value):
    """Return (target_table, row) or None for Kafka tombstone.

    JSON converter must be set with schemas.enable=false. We deliberately fail
    on unexpected messages rather than silently losing records.
    """
    if topic not in TOPIC_TO_TABLE:
        raise ValueError(f"Unexpected topic: {topic}")
    if value is None:
        return None
    event = json.loads(value)
    if not isinstance(event, dict) or event.get("op") not in ("r", "c", "u", "d"):
        raise ValueError(f"Unexpected Debezium envelope in {topic} offset={offset}; check schemas.enable=false")
    source = event.get("source") or {}
    before, after = event.get("before"), event.get("after")
    if event["op"] == "d" and before is None:
        raise ValueError(f"Delete without before image in {topic} offset={offset}; check REPLICA IDENTITY FULL")
    if event["op"] != "d" and after is None:
        raise ValueError(f"Missing after image in {topic} offset={offset}")
    row = {
        "kafka_topic": topic,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "kafka_ts_ms": kafka_ts_ms,
        "source_lsn": optional_number(source.get("lsn")),
        "source_ts_ms": optional_number(source.get("ts_ms")),
        "source_txid": optional_number(source.get("txId")),
        "op": event["op"],
        "before_json": _json(before),
        "after_json": _json(after),
    }
    return TOPIC_TO_TABLE[topic], row


def insert_batch(table, rows):
    if not rows:
        return
    base = os.getenv("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123").rstrip("/")
    # Topic mapping is fixed in code, not user-controlled.
    query = f"INSERT INTO {table} FORMAT JSONEachRow"
    url = base + "/?" + urlencode({"query": query, "async_insert": "0"})
    data = ("\n".join(_json(row) for row in rows) + "\n").encode("utf-8")
    username = os.getenv("CLICKHOUSE_USER", "zibal_analytics")
    password = os.environ["CLICKHOUSE_PASSWORD"]
    token = base64.b64encode((username + ":" + password).encode()).decode()
    req = Request(url, data=data, method="POST", headers={
        "Authorization": "Basic " + token,
        "Content-Type": "application/x-ndjson",
    })
    try:
        with urlopen(req, timeout=120) as response:
            response.read()  # full acknowledgment before Kafka offset commit
    except HTTPError as err:
        raise RuntimeError(f"ClickHouse HTTP {err.code}: {err.read(1024).decode(errors='replace')}") from err
    except URLError as err:
        raise RuntimeError(f"ClickHouse connection: {err}") from err


def self_test():
    topic = "zibal_cdc.core.transactions"
    sample = {"op":"r", "source":{"lsn":123,"ts_ms":1700000000000, "txId":None},
              "before":None, "after":{"transaction_id":8,"status":"SUCCESS"}}
    table, row = decode_event(topic, 0, 42, 1700000000000, json.dumps(sample).encode())
    assert table == "raw.cdc_transactions" and row["kafka_offset"] == 42
    assert row["before_json"] == "" and '"transaction_id":8' in row["after_json"]
    sample.update({"op":"u", "after":{"transaction_id":8,"installments":6}})
    _, evolved = decode_event(topic, 0, 43, -1, json.dumps(sample).encode())
    assert json.loads(evolved["after_json"])["installments"] == 6
    sample.update({"op":"d", "before":{"transaction_id":8}, "after":None})
    _, deleted = decode_event(topic, 0, 44, -1, json.dumps(sample).encode())
    assert deleted["op"] == "d" and deleted["after_json"] == ""
    assert decode_event(topic, 0, 45, -1, None) is None
    try:
        decode_event(topic, 0, 46, -1, b'{"schema":{},"payload":{}}')
        raise AssertionError("Unrecognized envelope must fail")
    except ValueError:
        pass
    print("PASS: snapshot, update/schema evolution, delete, tombstone, and malformed envelope")


def _stop(*_):
    global STOP
    STOP = True


def main():
    global STOP
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return

    # Import only when actually running; self-test does not need a broker.
    from confluent_kafka import Consumer, KafkaException

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    batch_size = int(os.getenv("CDC_BATCH_SIZE", "1000"))
    flush_seconds = float(os.getenv("CDC_FLUSH_SECONDS", "2"))
    if not 1 <= batch_size <= 10000:
        raise ValueError("CDC_BATCH_SIZE must be between 1 and 10000")
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "zibal_raw_clickhouse_v1"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": True,
        "max.poll.interval.ms": 900000,
        "session.timeout.ms": 45000,
    })
    topics = [name.strip() for name in os.getenv("KAFKA_TOPICS", ",".join(TOPIC_TO_TABLE)).split(",")]
    if set(topics) != set(TOPIC_TO_TABLE):
        raise ValueError(f"Expected topics {list(TOPIC_TO_TABLE)}, received {topics}")
    consumer.subscribe(topics)
    pending = {name: [] for name in TOPIC_TO_TABLE.values()}
    seen = 0
    last_flush = time.monotonic()
    try:
        while not STOP:
            msg = consumer.poll(0.2)
            if msg is not None:
                if msg.error():
                    raise KafkaException(msg.error())
                output = decode_event(msg.topic(), msg.partition(), msg.offset(),
                                      msg.timestamp()[1], msg.value())
                if output is not None:
                    table, row = output
                    pending[table].append(row)
                seen += 1
            if seen >= batch_size or (seen and time.monotonic() - last_flush >= flush_seconds):
                for table, rows in pending.items():
                    insert_batch(table, rows)
                # Persist Kafka positions ONLY once all ClickHouse table writes are acknowledged.
                consumer.commit(asynchronous=False)
                print(f"raw CDC persisted+committed: {seen} messages", flush=True)
                pending = {name: [] for name in TOPIC_TO_TABLE.values()}
                seen = 0
                last_flush = time.monotonic()
        if seen:
            for table, rows in pending.items():
                insert_batch(table, rows)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"FATAL CDC: {exc}", file=sys.stderr, flush=True)
        raise
