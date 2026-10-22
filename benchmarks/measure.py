"""Measures a PayStream run from the outside: what a user of the platform would see.

  * fraud quality: recall per scenario (from the generator's labels) and precision
  * alert latency: gateway receive -> alert raised, p50/p95/p99
  * throughput and consumer lag: from Prometheus (Flink + kafka-exporter)
  * lakehouse freshness: how far silver lags behind now, sampled every 10 s
  * exactly-once: Kafka records in == bronze rows, silver rows == distinct txn_ids, no duplicates

Usage (stack running, generator started separately or by run_benchmark.sh):
  python benchmarks/measure.py --duration 1800 --out benchmarks/runs/$(date +%s).json
  python benchmarks/measure.py --verify-only
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
import urllib.parse
import urllib.request
from collections import defaultdict

SCENARIO_RULE = {
    "velocity_burst": "VELOCITY",
    "account_takeover": "NEW_DEVICE_CASH_OUT",
    "mule_ring": "MULE_FAN_IN",
    "agent_float_spike": "AGENT_FLOAT_SPIKE",
    # analyst rules from rules/library, published at startup by the rules service
    "pin_brute_force": "failed-pin-burst",
    "device_farm": "device-farm",
    "structuring": "cash-in-structuring",
}


def rule_of(alert: dict) -> str:
    return alert.get("rule_id") or alert["rule"]


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    return s[min(len(s) - 1, int(q * len(s)))]


def prom(url: str, query: str) -> float | None:
    q = urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(f"{url}/api/v1/query?{q}", timeout=10) as r:
        result = json.load(r)["data"]["result"]
    return float(result[0]["value"][1]) if result else None


def trino_rows(host: str, sql: str) -> list:
    import trino

    cur = trino.dbapi.connect(host=host, port=8080, user="bench", catalog="lake").cursor()
    cur.execute(sql)
    return cur.fetchall()


def consume_all(bootstrap: str, registry: str, topic: str, group: str) -> list[dict]:
    """Reads a topic from the beginning to its current end."""
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    deser = AvroDeserializer(SchemaRegistryClient({"url": registry}))
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False,
                  "auto.offset.reset": "earliest"})
    parts = c.list_topics(topic, timeout=10).topics[topic].partitions
    ends = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    out, done = [], {p for p, end in ends.items() if end == 0}
    while len(done) < len(parts):
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        out.append(deser(msg.value(), SerializationContext(topic, MessageField.VALUE)))
        if msg.offset() >= ends[msg.partition()] - 1:
            done.add(msg.partition())
    c.close()
    return out


def topic_records(bootstrap: str, topic: str) -> int:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "bench-offsets"})
    parts = c.list_topics(topic, timeout=10).topics[topic].partitions
    total = 0
    for p in parts:
        low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        total += high - low
    c.close()
    return total


def fraud_quality(labels: list[dict], alerts: list[dict]) -> dict:
    shadow = [a for a in alerts if a.get("shadow")]
    alerts = [a for a in alerts if not a.get("shadow")]  # shadow rules are on trial: reported, not scored
    alerts_by = defaultdict(set)
    for a in alerts:
        alerts_by[(rule_of(a), a["account_id"])].add(a["alert_id"])
    labelled_accounts = {(SCENARIO_RULE[x["scenario"]], x["account_id"]) for x in labels}

    recall = {}
    for scenario, rule in SCENARIO_RULE.items():
        accounts = {x["account_id"] for x in labels if x["scenario"] == scenario}
        hit = sum(1 for acc in accounts if alerts_by.get((rule, acc)))
        recall[scenario] = {"incidents": len(accounts), "detected": hit,
                            "recall": round(hit / len(accounts), 4) if accounts else None}

    unique_alerts = {a["alert_id"]: a for a in alerts}.values()
    # an alert is right if its account is fraud of any kind: a takeover caught by the PIN rule still counts
    fraud_accounts = {x["account_id"] for x in labels}
    true_pos = sum(1 for a in unique_alerts if a["account_id"] in fraud_accounts)
    same_scenario = sum(1 for a in unique_alerts if (rule_of(a), a["account_id"]) in labelled_accounts)
    per_rule = defaultdict(int)
    for a in unique_alerts:
        per_rule[rule_of(a)] += 1
    latency = [(a["detected_at"] - a["ingest_time"]).total_seconds() * 1000 for a in unique_alerts
               if "detected_at" in a]  # a replay has no meaningful wall-clock latency
    return {
        "recall": recall,
        "alerts": len(unique_alerts),
        "alerts_per_rule": dict(sorted(per_rule.items())),
        "shadow_alerts": len({a["alert_id"] for a in shadow}),
        "precision": round(true_pos / len(unique_alerts), 4) if unique_alerts else None,
        "precision_same_scenario": round(same_scenario / len(unique_alerts), 4) if unique_alerts else None,
        "alert_latency_ms": {"p50": pct(latency, 0.5), "p95": pct(latency, 0.95), "p99": pct(latency, 0.99)},
    }


def exactly_once(bootstrap: str, trino_host: str, topic: str) -> dict:
    kafka_in = topic_records(bootstrap, topic)
    bronze = trino_rows(trino_host, "SELECT count(*) FROM lake.bronze.payment_events")[0][0]
    distinct_in_bronze = trino_rows(trino_host, "SELECT count(DISTINCT txn_id) FROM lake.bronze.payment_events")[0][0]
    silver, silver_distinct = trino_rows(
        trino_host, "SELECT count(*), count(DISTINCT txn_id) FROM lake.silver.payments")[0]
    return {
        "kafka_records": kafka_in,
        "bronze_rows": bronze,
        "silver_rows": silver,
        "silver_distinct_txn_ids": silver_distinct,
        "bronze_distinct_txn_ids": distinct_in_bronze,
        "no_loss_no_dup_in_bronze": kafka_in == bronze,
        "silver_is_deduplicated": silver == silver_distinct == distinct_in_bronze,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--duration", type=int, default=1800, help="seconds to sample metrics")
    p.add_argument("--bootstrap", default="localhost:9094")
    p.add_argument("--registry", default="http://localhost:8081")
    p.add_argument("--prometheus", default="http://localhost:9090")
    p.add_argument("--trino", default="localhost")
    p.add_argument("--settle", type=int, default=90, help="seconds to wait after sampling for the pipeline to drain")
    p.add_argument("--verify-only", action="store_true")
    p.add_argument("--out")
    a = p.parse_args()

    report: dict = {"started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    if not a.verify_only:
        rate, lag, freshness = [], [], []
        deadline = time.time() + a.duration
        while time.time() < deadline:
            rate.append(prom(a.prometheus, 'sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond'
                                           '{operator_name=~"Source:_kafka_payments.*"})') or 0.0)
            lag.append(prom(a.prometheus, 'sum(kafka_consumergroup_lag{consumergroup="paystream-flink"})') or 0.0)
            newest = trino_rows(a.trino, "SELECT to_unixtime(max(ingest_time)) FROM lake.silver.payments")[0][0]
            if newest:
                freshness.append(time.time() - newest)
            time.sleep(10)
        steady = rate[len(rate) // 10:] or rate  # skip warm-up
        report["throughput_events_per_s"] = {"mean": round(statistics.mean(steady), 1), "max": max(steady)}
        report["consumer_lag_records"] = {"p95": pct(lag, 0.95), "max": max(lag)}
        report["silver_freshness_s"] = {"p50": pct(freshness, 0.5), "p95": pct(freshness, 0.95)}
        report["checkpoint_duration_ms"] = prom(a.prometheus, "max(flink_jobmanager_job_lastCheckpointDuration)")
        report["checkpoint_size_bytes"] = prom(a.prometheus, "max(flink_jobmanager_job_lastCheckpointSize)")
        time.sleep(a.settle)

    labels = consume_all(a.bootstrap, a.registry, "fraud.labels", f"bench-labels-{time.time_ns()}")
    alerts = consume_all(a.bootstrap, a.registry, "fraud.alerts", f"bench-alerts-{time.time_ns()}")
    report["fraud"] = fraud_quality(labels, alerts)
    report["exactly_once"] = exactly_once(a.bootstrap, a.trino, "payments.events")

    text = json.dumps(report, indent=2, default=str)
    print(text)
    if a.out:
        with open(a.out, "w") as f:
            f.write(text + "\n")


if __name__ == "__main__":
    main()
