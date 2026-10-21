"""The decision log: every decision with the exact features it saw.

Training a model on features recomputed later from the lake risks training-serving skew (late
events, different window edges). Logging the served features with each decision, and joining the
outcome label later, gives training data that matches what the model will see in production.
"""

from __future__ import annotations

from collections import deque

import orjson


class KafkaDecisionLog:
    """Fire-and-forget: the payment path never waits on the log. Delivery failures are counted."""

    def __init__(self, bootstrap: str, topic: str = "payments.decisions"):
        from confluent_kafka import Producer

        self.topic = topic
        self.failures = 0
        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                                  "linger.ms": 20, "compression.type": "lz4", "client.id": "paystream-decision"})

    def _on_delivery(self, err, msg) -> None:
        if err:
            self.failures += 1

    def __call__(self, record: dict) -> None:
        self.producer.produce(self.topic, key=record["txn_id"].encode(), value=orjson.dumps(record),
                              on_delivery=self._on_delivery)
        self.producer.poll(0)

    def close(self) -> None:
        self.producer.flush(5)


class MemoryDecisionLog(deque):
    """Keeps the most recent decisions in memory: for tests, and for running without Kafka."""

    def __init__(self, maxlen: int = 10_000):
        super().__init__(maxlen=maxlen)

    def __call__(self, record: dict) -> None:
        self.append(record)

    def close(self) -> None:
        pass
