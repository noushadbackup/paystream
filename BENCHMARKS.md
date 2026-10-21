# Benchmarks

**Status: not measured yet.** The harness is ready; this page gets the numbers from the first real
runs. Every number below will come from a `benchmarks/runs/*.json` file committed next to it, with
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

## Results

| Run | Machine | Rate | Throughput | Alert p95 | Freshness p95 | Recall (min scenario) | Precision | Exactly-once |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| _none yet_ | | | | | | | | |
