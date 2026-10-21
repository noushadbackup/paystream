"""The effects of a case decision outside the database.

* Labels go to ``fraud.labels`` (Avro, the same contract the simulator writes), so analyst verdicts
  reach ``silver.fraud_labels`` and from there rule precision and model training.
* A freeze is a ``case-freeze`` entry in the ``case:{subject}`` hash. The decision service reads it on
  the next payment and declines. It lives apart from the ``flags:`` hash Flink writes, so a flag TTL
  set by the stream never shortens a freeze.
* Clearing a false positive deletes the flags the case's rules raised and adds ``ALLOW:{rule}``
  entries for the suppression period, so Flink re-flagging the same customer for the same pattern
  does not block them again.

Every effect is idempotent: the outbox may apply it more than once.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from paystream_cases.store import SUPPRESS_AFTER_FALSE_POSITIVE

FREEZE_FIELD = "case-freeze"
DEFAULT_FREEZE = 30 * 24 * 3600  # seconds; lifted earlier by an approver through the API


class KafkaLabels:
    def __init__(self, bootstrap: str, registry_url: str, schema_path: Path, topic: str = "fraud.labels"):
        from confluent_kafka import SerializingProducer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import StringSerializer

        registry = SchemaRegistryClient({"url": registry_url})
        self.topic = topic
        self.producer = SerializingProducer({
            "bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all",
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": AvroSerializer(registry, schema_path.read_text()),
        })

    def __call__(self, labels: list[dict]) -> None:
        errors: list = []
        for label in labels:
            self.producer.produce(self.topic, key=label["txn_id"], value=label,
                                  on_delivery=lambda err, _msg: errors.append(err) if err else None)
        if self.producer.flush(10) > 0:
            raise RuntimeError("timed out publishing labels")
        if errors:
            raise RuntimeError(f"Kafka rejected a label: {errors[0]}")


class MemoryLabels(list):
    def __call__(self, labels: list[dict]) -> None:
        self.extend(labels)


class AccountActions:
    """Writes what the decision service enforces. ``redis`` is a synchronous redis-py client."""

    def __init__(self, redis, freeze_s: int = DEFAULT_FREEZE,
                 allow_s: int = int(SUPPRESS_AFTER_FALSE_POSITIVE.total_seconds())):
        self.redis, self.freeze_s, self.allow_s = redis, freeze_s, allow_s

    def freeze(self, subject: str, case_id: str, now_ms: int | None = None) -> None:
        now_ms = now_ms or int(time.time() * 1000)
        entry = {"rule": FREEZE_FIELD, "action": "DECLINE", "case_id": case_id, "at": now_ms,
                 "until": now_ms + self.freeze_s * 1000}
        self._write(subject, {FREEZE_FIELD: entry})

    def unfreeze(self, subject: str) -> None:
        self.redis.hdel(f"case:{subject}", FREEZE_FIELD)

    def clear(self, subject: str, case_id: str, rules: list[str], now_ms: int | None = None) -> None:
        now_ms = now_ms or int(time.time() * 1000)
        allow = {f"ALLOW:{r}": {"rule": r, "action": "ALLOW", "case_id": case_id, "at": now_ms,
                                "until": now_ms + self.allow_s * 1000} for r in rules}
        pipe = self.redis.pipeline(transaction=True)
        if rules:
            pipe.hdel(f"flags:{subject}", *rules)
        pipe.hdel(f"case:{subject}", FREEZE_FIELD)
        self._write(subject, allow, pipe)
        pipe.execute()

    def _write(self, subject: str, entries: dict[str, dict], pipe=None) -> None:
        if not entries:
            return
        p = pipe or self.redis.pipeline(transaction=True)
        key = f"case:{subject}"
        p.hset(key, mapping={k: json.dumps(v, sort_keys=True) for k, v in entries.items()})
        # the key lives as long as its longest entry; GT never shortens a TTL another entry needs
        ttl = max(1, max(e["until"] for e in entries.values()) // 1000 - int(time.time()))
        p.expire(key, ttl, gt=True)
        p.expire(key, ttl, nx=True)
        if pipe is None:
            p.execute()


def resync(store, accounts: AccountActions, now=None) -> int:
    """Rewrite every account action in force from the case database; returns how many were applied."""
    from datetime import timedelta

    effects = Effects(lambda _labels: None, accounts)
    live = store.enforcement(timedelta(seconds=accounts.freeze_s), timedelta(seconds=accounts.allow_s), now)
    for payload in live:
        effects("CLOSE", payload)
    return len(live)


class Effects:
    """Applies outbox entries: ``CaseStore.dispatch(Effects(...))``."""

    def __init__(self, labels, accounts: AccountActions | None):
        self.labels, self.accounts = labels, accounts

    def __call__(self, kind: str, payload: dict) -> None:
        if kind == "CLOSE":
            if payload["labels"]:
                self.labels(payload["labels"])
            if self.accounts is None:
                return
            # expiry runs from when the case closed, so a retry or a resync never extends it
            at = payload.get("closed_at_ms")
            if payload["freeze"]:
                self.accounts.freeze(payload["subject_id"], payload["case_id"], at)
            if payload["clear_flags"]:
                self.accounts.clear(payload["subject_id"], payload["case_id"], payload["rules"], at)
        elif kind == "UNFREEZE":
            if self.accounts is not None:
                self.accounts.unfreeze(payload["subject_id"])
        else:
            raise ValueError(f"unknown effect {kind}")
