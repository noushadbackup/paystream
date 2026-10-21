"""paystream-gen: stream synthetic MFS payments to Kafka, a file, or the ledger.

Examples:
  paystream-gen --sink jsonl --out - --duration 5 --rate 50
  paystream-gen --sink kafka --rate 5000 --duration 1800 --realtime
  paystream-gen --sink kafka --start 2026-10-01T00:00:00Z --duration 86400 --rate 200 --diurnal
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime

from paystream_gen.population import Population
from paystream_gen.sinks import JsonlSink, KafkaSink, LedgerSink
from paystream_gen.traffic import Config, Simulator


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="paystream-gen", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sink", choices=["kafka", "jsonl"], default="jsonl")
    p.add_argument("--out", default="-", help="jsonl output path, - for stdout")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--registry", default="http://localhost:8081")
    p.add_argument("--topic", default="payments.events")
    p.add_argument("--labels-topic", default="fraud.labels")
    p.add_argument("--ledger-dsn", help="also post successful transactions to the Postgres ledger")
    p.add_argument("--rate", type=float, default=1000, help="mean events per second (before diurnal curve)")
    p.add_argument("--duration", type=int, default=60, help="simulated seconds")
    p.add_argument("--start", help="ISO start time for event_time; default now")
    p.add_argument("--realtime", action="store_true", help="pace output to the wall clock (benchmarks)")
    p.add_argument("--diurnal", action="store_true", help="apply the Dhaka daily traffic curve")
    p.add_argument("--users", type=int, default=200_000)
    p.add_argument("--merchants", type=int, default=20_000)
    p.add_argument("--agents", type=int, default=5_000)
    p.add_argument("--fraud-scale", type=float, default=1.0)
    p.add_argument("--p-late", type=float, default=0.01)
    p.add_argument("--p-duplicate", type=float, default=0.005)
    p.add_argument("--schema-version", type=int, choices=[1, 2], default=2)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = parse_args(argv)
    start_ms = int(datetime.fromisoformat(a.start).timestamp() * 1000) if a.start else int(time.time() * 1000)
    pop = Population.build(a.users, a.merchants, a.agents, seed=a.seed)
    cfg = Config(rate_per_s=a.rate, diurnal=a.diurnal, p_late=a.p_late, p_duplicate=a.p_duplicate,
                 fraud_scale=a.fraud_scale, schema_version=a.schema_version)
    sim = Simulator(pop, cfg, seed=a.seed)

    sinks = [KafkaSink(a.bootstrap, a.registry, a.topic, a.labels_topic, a.schema_version)
             if a.sink == "kafka" else JsonlSink(a.out)]
    if a.ledger_dsn:
        sinks.append(LedgerSink(a.ledger_dsn))

    wall0, sent, fraud, last_report = time.time(), 0, 0, time.time()
    try:
        for e in sim.run(start_ms, a.duration):
            if a.realtime:
                lag_s = (e.arrival_ms - start_ms) / 1000 - (time.time() - wall0)
                if lag_s > 0.002:
                    time.sleep(lag_s)
            for s in sinks:
                s.write(e)
            sent += 1
            fraud += e.scenario is not None
            if time.time() - last_report > 5:
                last_report = time.time()
                print(f"sent={sent:,} fraud={fraud:,} rate={sent / (last_report - wall0):,.0f}/s",
                      file=sys.stderr)
    except KeyboardInterrupt:
        pass
    finally:
        for s in sinks:
            s.close()
    elapsed = time.time() - wall0
    print(f"done: sent={sent:,} fraud_events={fraud:,} in {elapsed:.1f}s ({sent / max(elapsed, 1e-9):,.0f}/s)",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
