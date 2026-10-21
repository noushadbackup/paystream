# Benchmarks

**Status:** the decision API has a first measured run (below). The streaming pipeline numbers come
from the first full-stack runs. Every number below will come from a `benchmarks/runs/*.json` file committed next to it, with
the machine it ran on. Targets that are missed get reported as missed.

## How a run works

```bash
make up
RATE=5000 DURATION=1800 make bench            # steady load
CHAOS=1 RATE=5000 DURATION=1800 make bench    # same, with SIGKILLs to the TaskManager and broker
```

`run_benchmark.sh` starts the generator in real-time mode (1M users, 20k agents, seed 42) and
`measure.py` observes the system from outside: Prometheus for throughput, lag and checkpoints;
Trino for lakehouse freshness and row counts; the `fraud.labels` and `fraud.alerts` topics for
detection quality and latency.

## Targets

| Metric | Target | Measured from |
| --- | --- | --- |
| Sustained throughput | ≥ 5,000 events/s with lag not growing | `numRecordsOutPerSecond` of the Kafka source, consumer lag |
| Fraud alert latency | p95 < 2 s | `detected_at - ingest_time` on every alert |
| Silver freshness | p95 < 60 s | `now - max(ingest_time)` in `silver.payments`, every 10 s |
| Exactly-once under failure | Kafka records == bronze rows; silver rows == distinct txn_ids | `measure.py --verify-only` after a CHAOS run |
| Fraud recall | ≥ 95% per scenario, precision reported | generator ground-truth labels |
| Exactly-once cost | throughput and latency vs at-least-once | same run with `checkpointMs` and sink mode changed |
| Decision API latency | p99 < 50 ms at 1,000 decisions/s | `make decide-load`, client-side and server-side |

## Results

| Run | Machine | Rate | Throughput | Alert p95 | Freshness p95 | Recall (min scenario) | Precision | Exactly-once |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| _none yet_ | | | | | | | | |

### Decision API

| Run | Machine | Target rate | Achieved | Client p50 / p95 / p99 | Server p50 / p95 / p99 | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| [2026-10-21](benchmarks/results/decision-api-1000rps-devcontainer.json) | 4-vCPU shared cloud dev container; 2 uvicorn workers, local Redis 7 and the load client on the same box | 1,000/s for 60 s | 999/s | 1.8 / 5.3 / 48.0 ms | 0.7 / 3.1 / 25.7 ms | 0 |

Redis was filled with synthetic feature values of realistic size for the generator's 200,000
accounts, since this container has no Kafka or Flink; outcomes from that run are not meaningful,
latency is. p99 varied between 15 and 48 ms across 1,000/s runs on this shared machine, and the
two workers saturate at about 2,000/s here. The target is met at 1,000/s; the margin at p99 is thin
on this box, and the run on a dedicated machine with the full stack is the one to quote.
