"""Where generated events go: Kafka (Avro + Schema Registry), a JSONL file, or the Postgres ledger."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Protocol

from paystream_gen.traffic import Emitted

SCHEMA_DIR = Path(os.environ.get("PAYSTREAM_SCHEMA_DIR", Path(__file__).resolve().parents[3] / "schemas"))


class Sink(Protocol):
    def write(self, e: Emitted) -> None: ...
    def close(self) -> None: ...


class JsonlSink:
    def __init__(self, path: str):
        self._f = sys.stdout if path == "-" else open(path, "w")  # noqa: SIM115 - closed in close()

    def write(self, e: Emitted) -> None:
        self._f.write(json.dumps(e.event) + "\n")

    def close(self) -> None:
        if self._f is not sys.stdout:
            self._f.close()


class KafkaSink:
    """Avro-encoded, keyed by sender so one account's events stay ordered within a partition."""

    def __init__(self, bootstrap: str, registry_url: str, topic: str, labels_topic: str, schema_version: int):
        from confluent_kafka import SerializingProducer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import StringSerializer

        registry = SchemaRegistryClient({"url": registry_url})
        event_schema = (SCHEMA_DIR / ("payment_event.avsc" if schema_version >= 2
                                      else "history/payment_event.v1.avsc")).read_text()
        label_schema = (SCHEMA_DIR / "fraud_label.avsc").read_text()
        common = {
            "bootstrap.servers": bootstrap,
            "key.serializer": StringSerializer("utf_8"),
            "enable.idempotence": True,  # no duplicates from producer retries; the ones we inject are deliberate
            "acks": "all",
            "linger.ms": 20,
            "batch.size": 256 * 1024,
            "compression.type": "lz4",
        }
        self._events = SerializingProducer({**common, "value.serializer": AvroSerializer(registry, event_schema)})
        self._labels = SerializingProducer({**common, "value.serializer": AvroSerializer(registry, label_schema)})
        self._topic, self._labels_topic = topic, labels_topic

    def write(self, e: Emitted) -> None:
        ev = e.event
        while True:
            try:
                self._events.produce(self._topic, key=ev["sender_id"], value=ev)
                break
            except BufferError:  # local queue full: let the network catch up
                self._events.poll(0.05)
        self._events.poll(0)
        if e.scenario and not e.is_duplicate:
            self._labels.produce(self._labels_topic, key=ev["txn_id"],
                                 value={"txn_id": ev["txn_id"], "scenario": e.scenario, "account_id": e.subject})
            self._labels.poll(0)

    def close(self) -> None:
        self._events.flush(30)
        self._labels.flush(30)


class LedgerSink:
    """Posts successful transactions to the double-entry ledger, as the core banking system would.

    The ledger is a separate system of record: Debezium streams its changes to Kafka, and the
    daily reconciliation compares it with what the gateway stream says happened.
    """

    _SQL_TRANSFER = """
        INSERT INTO ledger.transfers (txn_id, txn_type, amount_paisa, event_time)
        VALUES (%s, %s, %s, to_timestamp(%s / 1000.0))
        ON CONFLICT (txn_id) DO NOTHING"""
    _SQL_ENTRY = """
        INSERT INTO ledger.entries (txn_id, leg, account_id, amount_paisa)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (txn_id, leg) DO NOTHING"""

    def __init__(self, dsn: str, batch: int = 500):
        import psycopg

        self._conn = psycopg.connect(dsn)
        self._batch, self._buf = batch, []

    def write(self, e: Emitted) -> None:
        if e.event["status"] == "SUCCESS" and not e.is_duplicate:
            self._buf.append(e.event)
            if len(self._buf) >= self._batch:
                self._flush()

    def _flush(self) -> None:
        if not self._buf:
            return
        with self._conn.cursor() as cur:
            cur.executemany(self._SQL_TRANSFER, [
                (ev["txn_id"], ev["txn_type"], ev["amount_paisa"], ev["event_time"]) for ev in self._buf])
            entries = []
            for ev in self._buf:
                entries.append((ev["txn_id"], "D", ev["sender_id"], -ev["amount_paisa"]))
                entries.append((ev["txn_id"], "C", ev["receiver_id"], ev["amount_paisa"]))
            cur.executemany(self._SQL_ENTRY, entries)
        self._conn.commit()  # the deferred balance check runs here
        self._buf.clear()

    def close(self) -> None:
        self._flush()
        self._conn.close()
