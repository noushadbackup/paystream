"""Scores a replay run (benchmarks/replay.sh): throughput, correctness of the tables against the input
file, and fraud recall and precision against the generator's labels.

  python benchmarks/replay_report.py --events E.jsonl --labels L.jsonl --run DIR [--note "..."]
"""

from __future__ import annotations

import argparse
import json
import os
import platform
from pathlib import Path

from measure import SCENARIO_RULE, fraud_quality, rule_of

# Fewest labelled events the generator emits for each scenario. An incident with fewer in the file
# was cut off by the start or end of the window and cannot show its full pattern.
MIN_EVENTS = {"velocity_burst": 8, "account_takeover": 1, "mule_ring": 13, "agent_float_spike": 30,
              "pin_brute_force": 5, "device_farm": 4, "structuring": 3}


def scored_recall(labels: list[dict], alerts: list[dict], label_time: dict[str, int], scored_from: int) -> dict:
    """Recall on incidents that start after the warm-up and are wholly inside the file."""
    flagged = {(rule_of(a), a["account_id"]) for a in alerts if not a.get("shadow")}
    out = {}
    for scenario, rule in SCENARIO_RULE.items():
        counts: dict[str, int] = {}
        starts: dict[str, int] = {}
        for x in labels:
            if x["scenario"] == scenario:
                acc = x["account_id"]
                counts[acc] = counts.get(acc, 0) + 1
                starts[acc] = min(starts.get(acc, label_time[x["txn_id"]]), label_time[x["txn_id"]])
        whole = [acc for acc, n in counts.items() if n >= MIN_EVENTS[scenario] and starts[acc] >= scored_from]
        hit = sum(1 for acc in whole if (rule, acc) in flagged)
        out[scenario] = {"incidents": len(whole), "detected": hit,
                         "recall": round(hit / len(whole), 4) if whole else None}
    return out


def machine() -> dict:
    cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")), platform.processor()) if Path("/proc/cpuinfo").exists() else ""
    mem_kb = next((int(line.split()[1]) for line in Path("/proc/meminfo").read_text().splitlines()
                   if line.startswith("MemTotal")), 0) if Path("/proc/meminfo").exists() else 0
    return {"cpu": cpu, "vcpus": os.cpu_count(), "ram_gb": round(mem_kb / 1024 / 1024, 1), "os": platform.platform()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--labels", required=True)
    ap.add_argument("--run", required=True, help="the --work directory of ReplayBenchmark")
    ap.add_argument("--users", type=int)
    ap.add_argument("--history-hours", type=int, default=0)
    ap.add_argument("--bootstrap-seconds", type=int, default=0)
    ap.add_argument("--warmup-minutes", type=int, default=30,
                    help="incidents starting this soon after the first event are not scored: rules with a"
                         " baseline (agent float) need that much traffic first")
    ap.add_argument("--note", default="")
    a = ap.parse_args()

    labels = [json.loads(x) for x in Path(a.labels).read_text().splitlines()]
    labelled = {x["txn_id"] for x in labels}
    lines, txn_ids, first, last, label_time, start = 0, set(), None, None, {}, None
    with open(a.events) as f:
        for line in f:
            e = json.loads(line)
            lines += 1
            txn_ids.add(e["txn_id"])
            t = e["event_time"]
            start = t if start is None else start  # the file is in arrival order; late events reach back
            first = t if first is None else min(first, t)
            last = t if last is None else max(last, t)
            if e["txn_id"] in labelled:
                label_time[e["txn_id"]] = t
    run = Path(a.run)
    flink = json.loads((run / "flink.json").read_text())
    alerts = [json.loads(x) for x in (run / "alerts.jsonl").read_text().splitlines()]
    c = flink["counts"]
    seconds = flink["net_runtime_ms"] / 1000

    report = {
        "kind": "replay",
        "note": a.note,
        "machine": machine(),
        "input": {"events": lines, "distinct_txn": len(txn_ids), "duplicates": lines - len(txn_ids),
                  "simulated_minutes": round((last - first) / 60_000, 1), "fraud_labels": len(labels),
                  "users": a.users, "history_hours": a.history_hours},
        "bootstrap_seconds": a.bootstrap_seconds,
        "flink": flink,
        "throughput_events_per_s": round(lines / seconds),
        "correctness": {
            "bronze_equals_input": c["bronze_rows"] == lines,
            "silver_equals_distinct_input": c["silver_distinct_txn"] == len(txn_ids),
            "silver_has_no_duplicates": c["silver_rows"] == c["silver_distinct_txn"],
        },
        "quality": fraud_quality(labels, alerts),
    }
    report["quality"].pop("alert_latency_ms")
    report["quality"]["warmup_minutes"] = a.warmup_minutes
    report["quality"]["recall_scored"] = scored_recall(labels, alerts, label_time, start + a.warmup_minutes * 60_000)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
