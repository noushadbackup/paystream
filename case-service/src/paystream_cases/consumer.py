"""Turns the alert and decision streams into cases, retries pending case effects, and periodically
rewrites the account actions in force into Redis (it may have restarted empty or evicted them).

At-least-once: offsets are committed only after the case database has committed what they carried.
Intake is idempotent on alert_id, so a redelivery after a crash attaches nothing twice.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable

from paystream_cases.actions import kafka_env
from paystream_cases.store import CaseStore

log = logging.getLogger("paystream.cases")

ALERTS, DECISIONS = "fraud.alerts", "payments.decisions"


def handle(store: CaseStore, topic: str, value) -> str | None:
    """One record from either topic. Returns the case it touched, if any."""
    if value is None:
        return None
    if topic == ALERTS:
        return store.ingest_alert(value)
    if topic == DECISIONS:
        return store.ingest_decision(json.loads(value) if isinstance(value, bytes | str) else value)
    raise ValueError(f"unexpected topic {topic}")


def run(store: CaseStore, bootstrap: str, registry_url: str, effects: Callable[[str, dict], None],
        group: str = "paystream-cases", commit_every: int = 500, dispatch_every_s: float = 1.0,
        stop: Callable[[], bool] = lambda: False, resync: Callable[[], int] | None = None,
        resync_every_s: float = 300.0) -> None:
    from confluent_kafka import DeserializingConsumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    alerts = AvroDeserializer(SchemaRegistryClient({"url": registry_url}))
    consumer = DeserializingConsumer({
        "bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False,
        "auto.offset.reset": "earliest", "isolation.level": "read_committed", **kafka_env(),
    })
    consumer.subscribe([ALERTS, DECISIONS])
    pending, last_dispatch, last_resync = 0, 0.0, 0.0
    try:
        while not stop():
            # the deserializer differs per topic, so decode here rather than in the consumer config
            msg = consumer.poll(0.5)
            if msg is not None and msg.error() is None:
                value = msg.value()
                if msg.topic() == ALERTS and value is not None:
                    value = alerts(value, SerializationContext(ALERTS, MessageField.VALUE))
                handle(store, msg.topic(), value)
                pending += 1
            elif msg is not None:
                log.warning("consumer error: %s", msg.error())
            if pending >= commit_every or (pending and msg is None):
                consumer.commit(asynchronous=False)
                pending = 0
            if time.monotonic() - last_dispatch >= dispatch_every_s:
                store.dispatch(effects)
                last_dispatch = time.monotonic()
            if resync and time.monotonic() - last_resync >= resync_every_s:  # also on start
                log.info("re-applied %d account actions from the case database", resync())
                last_resync = time.monotonic()
    finally:
        if pending:
            consumer.commit(asynchronous=False)
        consumer.close()
