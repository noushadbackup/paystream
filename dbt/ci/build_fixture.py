"""Builds a DuckDB file shaped like the lake's silver layer, from the real generator.

CI has no Kafka or Flink, so this stands in for them: it applies the same dedup Flink does
(leaving a few retries behind on purpose, for dbt to catch), posts successful payments to the
ledger as the generator's ledger sink does, and produces alerts two ways: the built-in detectors'
alerts are placed where those rules fire, and the analyst rules in rules/library are run for real
through the rules service's engine, which Flink matches on the shared conformance vectors.
"""

import argparse
import json
import zlib
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from paystream_gen.population import Population
from paystream_gen.traffic import Config, Simulator
from paystream_rules.engine import Engine
from paystream_rules.rule import validate

LIBRARY = Path(__file__).resolve().parents[2] / "rules" / "library"

RULES = {
    "velocity_burst": ("VELOCITY", 6),
    "mule_ring": ("MULE_FAN_IN", 8),
    "account_takeover": ("NEW_DEVICE_CASH_OUT", 1),
    "agent_float_spike": ("AGENT_FLOAT_SPIKE", 25),
}  # the other scenarios are caught by analyst rules, run below
DETAILS = {
    "VELOCITY": "{n} outgoing transfers in 60s",
    "MULE_FAN_IN": "{n} distinct senders in 10 min",
    "NEW_DEVICE_CASH_OUT": "large cash-out from a new device",
    "AGENT_FLOAT_SPIKE": "{n} cash-outs in 10 min vs baseline",
}


def ts(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


def insert(con: duckdb.DuckDBPyConnection, table: str, rows: list[tuple]) -> None:
    """Bulk insert through a DataFrame: orders of magnitude faster than executemany in DuckDB."""
    cols = [r[0] for r in con.execute(f"describe {table}").fetchall()]
    frame = pd.DataFrame(rows, columns=cols)  # noqa: F841 - DuckDB reads it by name below
    con.execute(f"insert into {table} select * from frame")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="ci/ci.duckdb")
    p.add_argument("--seconds", type=int, default=1800)
    a = p.parse_args()

    start = int(datetime(2026, 10, 1, 23, 40, tzinfo=UTC).timestamp() * 1000)  # crosses midnight
    sim = Simulator(Population.build(20_000, 500, 200, seed=11), Config(rate_per_s=50, fraud_scale=4), seed=11)

    payments, transfers, entries, alerts, labels, seen, leaked = [], [], [], [], [], set(), 0
    library = [validate(json.loads(f.read_text())) for f in sorted(LIBRARY.glob("*.json"))]
    versions = {r["rule_id"]: r for r in library}
    engine = Engine(library)
    fraud_counts: dict[tuple[str, str], int] = {}
    end = start + a.seconds * 1000
    for e in sim.run(start, a.seconds):
        ev = e.event
        if ev["event_time"] >= end:  # scenarios scheduled past the end of the run
            continue
        row = (ev["txn_id"], ts(ev["event_time"]), ev["txn_type"], ev["sender_id"], ev["receiver_id"],
               ev["amount_paisa"], ev["currency"], ev["device_id"], ev["district"], ev["status"],
               ev.get("channel"), ts(e.arrival_ms + (5 if e.is_duplicate else 0)))
        if ev["txn_id"] in seen:
            if leaked < 3:  # a retry that outlived Flink's dedup state
                payments.append(row)
                leaked += 1
            continue
        seen.add(ev["txn_id"])
        payments.append(row)
        for hit in engine.process(ev):
            latency_ms = 150 + zlib.crc32(hit.trigger_txn_id.encode()) % 700
            rule = versions[hit.rule_id]
            alerts.append((f"{hit.rule_id}:{hit.key}:{hit.trigger_txn_id}", "DYNAMIC", hit.key, hit.trigger_txn_id,
                           ts(ev["event_time"]), ts(e.arrival_ms), ts(e.arrival_ms + latency_ms),
                           hit.observed / rule["threshold"]["value"],
                           f"{rule['name']}: {rule['aggregate']['fn']} = {hit.observed:g}",
                           hit.rule_id, hit.rule_version, hit.shadow, hit.action))
        if ev["status"] == "SUCCESS":
            posted = ts(e.arrival_ms + 50)
            transfers.append((ev["txn_id"], ev["txn_type"], ev["amount_paisa"], ts(ev["event_time"]), posted))
            entries.append((ev["txn_id"], "D", ev["sender_id"], -ev["amount_paisa"], posted))
            entries.append((ev["txn_id"], "C", ev["receiver_id"], ev["amount_paisa"], posted))
        if e.scenario and not e.is_duplicate:
            labels.append((ev["txn_id"], e.scenario, e.subject, ts(e.arrival_ms)))
        if e.scenario in RULES and not e.is_duplicate:
            # one alert per incident, raised where the Flink rule would fire (see RuleConfig)
            key = (e.scenario, e.subject)
            fraud_counts[key] = fraud_counts.get(key, 0) + 1
            rule, fires_at = RULES[e.scenario]
            if fraud_counts[key] == fires_at:
                latency_ms = 120 + zlib.crc32(ev["txn_id"].encode()) % 900
                alerts.append((f"{rule}:{e.subject}:{ev['txn_id']}", rule, e.subject, ev["txn_id"],
                               ts(ev["event_time"]), ts(e.arrival_ms), ts(e.arrival_ms + latency_ms),
                               1.0 + zlib.crc32(e.subject.encode()) % 100 / 100, DETAILS[rule].format(n=fires_at),
                               None, None, False, "REVIEW"))

    con = duckdb.connect(a.out)
    con.execute("create schema if not exists silver")
    con.execute("""create or replace table silver.payments (txn_id varchar, event_time timestamptz, txn_type varchar,
        sender_id varchar, receiver_id varchar, amount_paisa bigint, currency varchar, device_id varchar,
        district varchar, status varchar, channel varchar, ingest_time timestamptz)""")
    insert(con, "silver.payments", payments)
    con.execute("""create or replace table silver.ledger_transfers (txn_id varchar, txn_type varchar,
        amount_paisa bigint, event_time timestamptz, posted_at timestamptz)""")
    insert(con, "silver.ledger_transfers", transfers)
    con.execute("""create or replace table silver.ledger_entries (txn_id varchar, leg varchar, account_id varchar,
        amount_paisa bigint, posted_at timestamptz)""")
    insert(con, "silver.ledger_entries", entries)
    con.execute("""create or replace table silver.fraud_alerts (alert_id varchar, rule varchar, account_id varchar,
        trigger_txn_id varchar, event_time timestamptz, ingest_time timestamptz, detected_at timestamptz,
        score double, details varchar, rule_id varchar, rule_version integer, shadow boolean, action varchar)""")
    insert(con, "silver.fraud_alerts", alerts)
    con.execute("""create or replace table silver.fraud_labels (txn_id varchar, scenario varchar, account_id varchar,
        labelled_at timestamptz)""")
    insert(con, "silver.fraud_labels", labels)
    print(f"fixture: {len(payments)} payments ({leaked} leaked retries), {len(transfers)} transfers, "
          f"{len(alerts)} alerts, {len(labels)} labels -> {a.out}")


if __name__ == "__main__":
    main()
