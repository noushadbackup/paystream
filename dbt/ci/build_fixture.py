"""Builds a DuckDB file shaped like the lake's silver layer, from the real generator.

CI has no Kafka or Flink, so this stands in for them: it applies the same dedup Flink does
(leaving a few retries behind on purpose, for dbt to catch), posts successful payments to the
ledger as the generator's ledger sink does, and turns fraud labels into alerts.
"""

import argparse
from datetime import UTC, datetime

import duckdb

from paystream_gen.population import Population
from paystream_gen.traffic import Config, Simulator


def ts(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="ci/ci.duckdb")
    p.add_argument("--seconds", type=int, default=600)
    a = p.parse_args()

    start = int(datetime(2026, 10, 1, 23, 55, tzinfo=UTC).timestamp() * 1000)  # crosses midnight
    sim = Simulator(Population.build(20_000, 500, 200, seed=11), Config(rate_per_s=50, fraud_scale=20), seed=11)

    payments, transfers, entries, alerts, seen, leaked = [], [], [], [], set(), 0
    for e in sim.run(start, a.seconds):
        ev = e.event
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
        if ev["status"] == "SUCCESS":
            posted = ts(e.arrival_ms + 50)
            transfers.append((ev["txn_id"], ev["txn_type"], ev["amount_paisa"], ts(ev["event_time"]), posted))
            entries.append((ev["txn_id"], "D", ev["sender_id"], -ev["amount_paisa"], posted))
            entries.append((ev["txn_id"], "C", ev["receiver_id"], ev["amount_paisa"], posted))
        if e.scenario:
            alerts.append((f"{e.scenario}:{ev['txn_id']}", "VELOCITY" if e.scenario == "velocity_burst" else
                           "MULE_FAN_IN" if e.scenario == "mule_ring" else
                           "NEW_DEVICE_CASH_OUT" if e.scenario == "account_takeover" else "AGENT_FLOAT_SPIKE",
                           ev["sender_id"], ev["txn_id"], ts(ev["event_time"]), ts(e.arrival_ms),
                           ts(e.arrival_ms + 300), 1.2, e.scenario))

    con = duckdb.connect(a.out)
    con.execute("create schema if not exists silver")
    con.execute("""create or replace table silver.payments (txn_id varchar, event_time timestamptz, txn_type varchar,
        sender_id varchar, receiver_id varchar, amount_paisa bigint, currency varchar, device_id varchar,
        district varchar, status varchar, channel varchar, ingest_time timestamptz)""")
    con.executemany("insert into silver.payments values (?,?,?,?,?,?,?,?,?,?,?,?)", payments)
    con.execute("""create or replace table silver.ledger_transfers (txn_id varchar, txn_type varchar,
        amount_paisa bigint, event_time timestamptz, posted_at timestamptz)""")
    con.executemany("insert into silver.ledger_transfers values (?,?,?,?,?)", transfers)
    con.execute("""create or replace table silver.ledger_entries (txn_id varchar, leg varchar, account_id varchar,
        amount_paisa bigint, posted_at timestamptz)""")
    con.executemany("insert into silver.ledger_entries values (?,?,?,?,?)", entries)
    con.execute("""create or replace table silver.fraud_alerts (alert_id varchar, rule varchar, account_id varchar,
        trigger_txn_id varchar, event_time timestamptz, ingest_time timestamptz, detected_at timestamptz,
        score double, details varchar)""")
    con.executemany("insert into silver.fraud_alerts values (?,?,?,?,?,?,?,?,?)", alerts)
    print(f"fixture: {len(payments)} payments ({leaked} leaked retries), {len(transfers)} transfers, "
          f"{len(alerts)} alerts -> {a.out}")


if __name__ == "__main__":
    main()
