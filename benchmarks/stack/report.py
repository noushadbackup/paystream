"""Metrics for the native-stack benchmark (benchmarks/stack/run.sh).

  report.py sample --job ID --bootstrap B --out samples.jsonl   every 5 s until killed
  report.py drain  --job ID [--timeout 900]                     waits until the Kafka backlog is consumed
  report.py final  --run DIR --bootstrap B ...                 prints the result JSON

Samples come from the Flink REST API (records/s out of the Kafka source, records still pending in
Kafka per the source's own metric, checkpoints) so no Prometheus is needed.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from measure import fraud_quality, pct
from replay_report import machine, scored_recall

REST = os.environ.get("FLINK_REST", "http://localhost:18082")
SOURCE_METRICS = ["Source__kafka-payments.numRecordsOutPerSecond", "Source__kafka-payments.pendingRecords"]


def get(path: str):
    with urllib.request.urlopen(REST + path, timeout=10) as r:
        return json.load(r)


def source_vertex(job: str) -> str:
    return next(v["id"] for v in get(f"/jobs/{job}")["vertices"] if v["name"].startswith("Source: kafka-payments"))


def metric_sum(job: str, vertex: str, names: list[str]) -> dict[str, float]:
    rows = get(f"/jobs/{job}/vertices/{vertex}/subtasks/metrics?get={','.join(names)}&agg=sum")
    return {r["id"]: r.get("sum") for r in rows}


def sample(a) -> None:
    vertex = None
    with open(a.out, "a") as out:
        while True:
            row: dict = {"t": time.time()}
            try:
                job = get(f"/jobs/{a.job}")
                row["state"] = job["state"]
                vertex = vertex or source_vertex(a.job)
                m = metric_sum(a.job, vertex, SOURCE_METRICS)
                row["source_rate"] = m.get(SOURCE_METRICS[0])
                row["pending"] = m.get(SOURCE_METRICS[1])
                cp = get(f"/jobs/{a.job}/checkpoints")
                row["checkpoints_completed"] = cp["counts"]["completed"]
                row["checkpoints_failed"] = cp["counts"]["failed"]
                last = (cp.get("latest") or {}).get("completed")
                if last:
                    row["checkpoint"] = {"id": last["id"], "duration_ms": last["end_to_end_duration"],
                                         "checkpointed_bytes": last.get("checkpointed_size"),
                                         "state_bytes": last.get("state_size")}
                restarts = get(f"/jobs/{a.job}/metrics?get=numRestarts")
                row["restarts"] = float(restarts[0]["value"]) if restarts else None
            except (OSError, ValueError, KeyError, StopIteration) as e:  # a TaskManager kill is part of the run
                row["error"] = str(e)[:200]
            out.write(json.dumps(row) + "\n")
            out.flush()
            time.sleep(5)


def drain(a) -> None:
    deadline = time.time() + a.timeout
    vertex = source_vertex(a.job)
    quiet = 0
    while time.time() < deadline:
        try:
            m = metric_sum(a.job, vertex, SOURCE_METRICS)
            pending, rate = m.get(SOURCE_METRICS[1]) or 0, m.get(SOURCE_METRICS[0]) or 0
            quiet = quiet + 1 if pending == 0 and rate == 0 else 0
            if quiet >= 3:
                time.sleep(25)  # two more checkpoints, so everything read is committed
                return
        except (OSError, ValueError, KeyError):
            quiet = 0
        time.sleep(5)
    raise SystemExit("job did not drain in time")


def topic_end_offsets(bootstrap: str, topic: str) -> int:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "bench-offsets"})
    parts = c.list_topics(topic, timeout=10).topics[topic].partitions
    total = sum(c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts)
    c.close()
    return total


def read_topic(bootstrap: str, topic: str, schema: Path, fields: list[str]) -> list[dict]:
    """Every record of a topic, keeping `fields` and the Kafka timestamp. Decoded with the schema file the
    producer wrote with (the generator's), after the 5-byte registry header, so it needs no registry."""
    import io

    import fastavro
    from confluent_kafka import Consumer, TopicPartition

    parsed = fastavro.parse_schema(json.loads(schema.read_text()))
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"bench-{time.time_ns()}",
                  "enable.auto.commit": False})
    parts = c.list_topics(topic, timeout=10).topics[topic].partitions
    ends = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    out, done = [], {p for p, end in ends.items() if end == 0}
    while len(done) < len(parts):
        for msg in c.consume(10_000, timeout=1.0):
            if msg.error():
                continue
            v = fastavro.schemaless_reader(io.BytesIO(msg.value()[5:]), parsed)
            row = {f: v.get(f) for f in fields}
            row["_ts"] = msg.timestamp()[1]
            out.append(row)
            if msg.offset() >= ends[msg.partition()] - 1:
                done.add(msg.partition())
    c.close()
    return out


def final(a) -> None:
    run = Path(a.run)
    samples = [json.loads(x) for x in (run / "samples.jsonl").read_text().splitlines()]
    lake = json.loads((run / "lake.json").read_text())
    alerts = [json.loads(x) for x in (run / "alerts.jsonl").read_text().splitlines()]
    chaos = [json.loads(x) for x in (run / "chaos.jsonl").read_text().splitlines()] \
        if (run / "chaos.jsonl").exists() else []

    schemas = Path(__file__).resolve().parents[2] / "schemas"
    events = read_topic(a.bootstrap, "payments.events", schemas / "payment_event.avsc", ["txn_id"])
    labels = read_topic(a.bootstrap, "fraud.labels", schemas / "fraud_label.avsc", ["txn_id", "scenario", "account_id"])
    produced = len(events)
    distinct = len({e["txn_id"] for e in events})
    first_ts = min(e["_ts"] for e in events)
    arrival = {e["txn_id"]: e["_ts"] for e in events}
    label_time = {x["txn_id"]: arrival.get(x["txn_id"], x["_ts"]) for x in labels}

    running = [s for s in samples if s.get("state") == "RUNNING" and s.get("source_rate") is not None]
    main_end = first_ts + a.duration * 1000  # after it only late events and scenario tails trickle in
    in_main = sum(1 for e in events if e["_ts"] <= main_end)
    t0, t1 = first_ts / 1000 + 60, main_end / 1000  # skip the first minute of ramp-up
    steady = [s for s in running if t0 <= s["t"] <= t1]
    rates = [s["source_rate"] for s in steady]
    pending = [s["pending"] for s in steady if s.get("pending") is not None]
    cps = {s["checkpoint"]["id"]: s["checkpoint"] for s in samples if s.get("checkpoint")}
    durations = [c["duration_ms"] for c in cps.values()]
    latency = [x["latency_ms"] for x in alerts if "latency_ms" in x and not x.get("shadow")]
    restarts = max((s.get("restarts") or 0 for s in samples), default=0)
    by_window: dict[int, list[int]] = {}
    for x in alerts:
        if "detected_at_ms" in x and not x.get("shadow"):
            by_window.setdefault((x["detected_at_ms"] - first_ts) // 600_000, []).append(x["latency_ms"])
    cp_by_window: dict[int, list[int]] = {}
    for c in cps.values():
        s = next((s for s in samples if s.get("checkpoint", {}).get("id") == c["id"]), None)
        if s:
            cp_by_window.setdefault(int((s["t"] * 1000 - first_ts) // 600_000), []).append(c["duration_ms"])

    quality = fraud_quality(labels, alerts)
    quality.pop("alert_latency_ms")
    quality["warmup_minutes"] = a.warmup_minutes
    quality["recall_scored"] = scored_recall(labels, alerts, label_time, first_ts + a.warmup_minutes * 60_000)
    c = lake["counts"]
    fresh = lake.get("silver_freshness", {})
    report = {
        "kind": "stack",
        "note": a.note,
        "machine": machine(),
        "setup": {"kafka": "3.8.1, one broker (KRaft)", "flink": "1.20.1 standalone, 1 JobManager + 1 TaskManager",
                  "state_backend": "rocksdb, incremental checkpoints every 10 s, exactly-once",
                  "lakehouse": "Iceberg, Hadoop catalog on local disk", "online_store": "Redis",
                  "partitions": a.partitions, "parallelism": a.parallelism, "users": a.users,
                  "target_rate": a.rate, "duration_s": a.duration, "chaos": chaos,
                  "bootstrap_seconds": int((run / "bootstrap_seconds").read_text())
                  if (run / "bootstrap_seconds").exists() else None,
                  "python": platform.python_version()},
        "throughput": {"produced_events": produced, "produced_per_s": round(in_main / a.duration),
                       "tail_events_after_duration": produced - in_main,
                       "flink_source_per_s_mean": round(statistics.mean(rates), 1) if rates else None,
                       "flink_source_per_s_max": round(max(rates), 1) if rates else None},
        "consumer_lag_records": {"p50": pct(pending, 0.5), "p95": pct(pending, 0.95), "max": max(pending, default=None)},
        "alert_latency_ms": {"alerts": len(latency), "p50": pct(latency, 0.5), "p95": pct(latency, 0.95),
                             "p99": pct(latency, 0.99), "max": max(latency, default=None)},
        "by_10_minutes": [{"minutes": f"{10 * w}-{10 * w + 10}", "alerts": len(v), "alert_latency_p95_ms": pct(v, 0.95),
                           "checkpoint_ms_max": max(cp_by_window.get(w, [0]))} for w, v in sorted(by_window.items())],
        "silver_freshness_ms": {k: fresh.get(k) for k in ("commits", "p50_ms", "p95_ms", "max_ms", "definition")},
        "checkpoints": {"completed": len(cps), "failed": max((s.get("checkpoints_failed") or 0 for s in samples), default=0),
                        "duration_ms_p50": pct(durations, 0.5), "duration_ms_max": max(durations, default=None),
                        "state_bytes_last": cps[max(cps)]["state_bytes"] if cps else None},
        "restarts": restarts,
        "exactly_once": {
            "kafka_records": produced, "bronze_rows": c["bronze_rows"],
            "bronze_equals_kafka": c["bronze_rows"] == produced,
            "distinct_txn_in_kafka": distinct, "silver_rows": c["silver_rows"],
            "silver_equals_distinct": c["silver_rows"] == distinct and c["silver_distinct_txn"] == distinct,
        },
        "quality": quality,
    }
    print(json.dumps(report, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sample")
    s.add_argument("--job", required=True)
    s.add_argument("--bootstrap")
    s.add_argument("--out", required=True)
    d = sub.add_parser("drain")
    d.add_argument("--job", required=True)
    d.add_argument("--timeout", type=int, default=900)
    f = sub.add_parser("final")
    f.add_argument("--run", required=True)
    f.add_argument("--bootstrap", required=True)
    for name in ("rate", "duration", "users", "partitions", "parallelism", "chaos"):
        f.add_argument(f"--{name}", type=int)
    f.add_argument("--note", default="")
    f.add_argument("--warmup-minutes", type=int, default=30,
                   help="incidents starting earlier are not scored: rules with baselines need traffic first")
    a = ap.parse_args()
    {"sample": sample, "drain": drain, "final": final}[a.cmd](a)


if __name__ == "__main__":
    main()
