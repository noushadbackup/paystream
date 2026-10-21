"""Where approved rules go: the compacted Kafka topic Flink reads them from."""

from __future__ import annotations

import json
import os


def kafka_env(environ=None) -> dict:
    """Client settings from KAFKA_CFG_* variables, the convention every PayStream service shares:
    KAFKA_CFG_SECURITY_PROTOCOL=SASL_SSL becomes security.protocol. Secrets stay in the environment."""
    environ = os.environ if environ is None else environ
    return {k[len("KAFKA_CFG_"):].lower().replace("_", "."): v for k, v in environ.items()
            if k.startswith("KAFKA_CFG_") and len(k) > len("KAFKA_CFG_")}


class KafkaPublisher:
    """Synchronous, idempotent produce with acks=all: approval returns only once the rule is durable."""

    def __init__(self, bootstrap: str, topic: str = "fraud.rules"):
        from confluent_kafka import Producer

        self.topic = topic
        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all",
                                  "client.id": "paystream-rules", **kafka_env()})

    def __call__(self, rule: dict) -> None:
        errors: list = []
        self.producer.produce(self.topic, key=rule["rule_id"].encode(),
                              value=json.dumps(rule, sort_keys=True).encode(),
                              on_delivery=lambda err, msg: errors.append(err) if err else None)
        if self.producer.flush(10) > 0:
            raise RuntimeError(f"timed out publishing {rule['rule_id']} v{rule['version']}")
        if errors:
            raise RuntimeError(f"Kafka rejected {rule['rule_id']} v{rule['version']}: {errors[0]}")


class MemoryPublisher(list):
    """For tests and for running the service without Kafka."""

    def __call__(self, rule: dict) -> None:
        self.append(rule)
