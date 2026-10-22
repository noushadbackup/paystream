# Benchmarks

**Status:** three measured benchmarks so far, each below with its result files: the live stack
(Kafka and a Flink cluster, an hour at 2,000 payments/s and a run with the TaskManager killed twice),
the replay benchmark (the production Flink job over 7.2 million payments from 5 million accounts) and
the decision API. The Docker Compose run (`make bench`, next section) has not been run yet.
Every number here comes from a file in `benchmarks/results/` with the machine it ran on, and
targets that are missed get reported as missed.

## How a full-stack run works

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

### Full stack

| Run | Machine | Rate | Throughput | Alert p95 | Freshness p95 | Recall (min scenario) | Precision | Exactly-once |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| _not run yet_ | | | | | | | | |

## Live stack benchmark

```bash
KAFKA_HOME=... FLINK_HOME=... RATE=2000 DURATION=3600 USERS=5000000 \
  HISTORY=benchmarks/replay-work/.../history.jsonl ./benchmarks/stack/run.sh
CHAOS=1 DURATION=1200 ...   # same, with kill -9 of the TaskManager at one and two thirds
```

[`stack/run.sh`](benchmarks/stack/run.sh) runs the production job on real infrastructure without
Docker: a Kafka 3.8 broker (KRaft, 8 partitions), a standalone Flink 1.20 cluster (one JobManager,
one TaskManager with 4 slots, RocksDB, incremental exactly-once checkpoints every 10 s), Redis for
the online store and Iceberg tables in a Hadoop catalog on local disk. It bootstraps two hours of
device history into a savepoint, submits the same jar `make up` deploys, and has the generator
produce into Kafka in real time. [`stack/report.py`](benchmarks/stack/report.py) samples the Flink
REST API every 5 s and afterwards checks the lakehouse against every record in the topic. Alert
latency is `detected_at - ingest_time` on every alert, where `ingest_time` is the payment's Kafka
timestamp. Silver freshness comes from Iceberg's own metadata: for each commit, commit time minus
the earliest `ingest_time` among the rows it made visible.

Everything, generator included, shares one 4-vCPU, 15.7 GB cloud container (Intel Xeon 2.1 GHz),
so 2,000 events/s is a rate this box can sustain for an hour with headroom, not the job's ceiling.
The replay above measures the ceiling of the Flink job alone (4,334 events/s on the same box).

| Run | Load | Lag max | Alert p50 / p95 / p99 / max | Checkpoints p50 / max | Freshness p95 | Exactly-once |
| --- | --- | --- | --- | --- | --- | --- |
| [steady](benchmarks/results/stack-20261022T034821Z.json) | 2,010/s for 60 min, 7,238,673 events, 5M users | **0 records** | **123 / 226 / 477 / 622 ms** | 1.1 / 1.7 s at 1.56 GB state | **11.8 s** | bronze 7,238,673 == Kafka; silver 7,202,657 == distinct |
| [chaos](benchmarks/results/stack-20261022T050237Z.json) | 2,008/s for 20 min, TaskManager `kill -9` twice | 17,331 | 130 / 20,444 / 27,466 / 29,283 ms | 0.9 / 6.5 s | 11.9 s (max 45 s) | bronze 2,412,034 == Kafka; silver 2,400,061 == distinct |
| [first steady, glibc malloc](benchmarks/results/stack-20261022T015446Z.json) | 2,010/s for 60 min | 12,307 | 148 / 22,137 / 40,668 / 44,415 ms | 1.2 / 25.1 s | 40 s | held |

**Steady.** For the whole hour the consumer kept up with the producer (source lag 0 at every
sample), no restarts, and alert latency by 10-minute window stayed between 200 and 300 ms at p95
while RocksDB state grew to 1.56 GB. Silver freshness sits just above the 10 s checkpoint interval,
which is the floor for exactly-once Iceberg commits. Precision was 82.4% and every scored incident
was detected except 9 of 59 account takeovers, the same picture as the replay.

**Chaos.** The TaskManager was killed with `kill -9` at 6:40 and 13:20 and restarted 10 s later.
Each time the job restored from the last checkpoint, reprocessed from the committed offsets and
cleared the backlog within 48 s and 57 s of the kill (peak backlog 17,331 records, catch-up at up
to 2,946 events/s). Bronze still equals the topic record for record and silver still equals the
distinct transactions: no loss and no duplicates across two crashes. Payments that arrived while
the job was down were alerted on up to 29 s late; the p95 above is those alerts.

**What the first steady run found.** With the JVM's default glibc malloc the job held p95 under
300 ms for 48 minutes, then degraded: once state passed about 1.3 GB, checkpoints went from 1 s to
18 to 25 s and the job fell behind. The TaskManager's resident memory reached 8.1 GB against a 6.5 GB
budget, and the container's memory limit killed it during the final savepoint (after the data was
committed, so exactly-once still held). RocksDB allocates natively, and glibc's per-thread arenas
fragment that memory. The rerun preloads jemalloc, which the official Flink image (the base of
ours) enables by default: same data, same state size, the TaskManager at about 4.5 GB when sampled
at minutes 25 and 54, and checkpoints under 1.8 s for the whole hour. `run.sh` now preloads it too.

Setting the cluster up also exposed a packaging bug the local tests could not: the job jar bundled
the Hadoop client, which Flink loads parent-first, so on a real cluster the Iceberg sink failed with
`NoClassDefFoundError`. The Hadoop client now goes into Flink's `lib/` (the Dockerfile copies it)
and is `provided` in the job.

## Replay benchmark

```bash
RATE=2000 DURATION=3600 USERS=5000000 HISTORY_HOURS=2 ./benchmarks/replay.sh
```

[`replay.sh`](benchmarks/replay.sh) generates an hour of traffic (5 million users, 20,000 agents,
late and duplicate events, labelled fraud) plus two earlier hours of history, writes that history
into a savepoint with [`DeviceHistoryBootstrap`](flink-jobs/src/main/java/io/paystream/flink/bootstrap/DeviceHistoryBootstrap.java)
([ADR 13](docs/adr/0013-warm-start-from-the-lakehouse.md)), and runs the production
`PaymentsPipeline` from that savepoint as fast as it will go: RocksDB state with incremental
checkpoints every 10 s, exactly-once Iceberg sinks for bronze, silver, alerts and KPIs, online
features written to Redis, the four built-in detectors and the analyst rule library.
[`replay_report.py`](benchmarks/replay_report.py) then checks the lakehouse against the input and
scores every alert against the generator's labels.

What it is and is not: the source is a file read in arrival order instead of Kafka, so it measures
the Flink job, its state and its lakehouse writes, not the brokers. Everything runs on one 4-vCPU,
15.7 GB cloud container (Intel Xeon 2.1 GHz) at parallelism 4.

### Final run

[`replay-20261022T003309Z.json`](benchmarks/results/replay-20261022T003309Z.json)

| Input | |
| --- | --- |
| Events (arrival order, 87.6 simulated minutes including late arrivals) | 7,239,318 |
| Distinct transactions (the rest are app retries) | 7,203,302 |
| Accounts | 5,000,000 users, 20,000 agents |
| Labelled fraud events | 2,921 |
| History bootstrapped into the savepoint | 2 hours, 181 s to build |

| Result | |
| --- | --- |
| Throughput | **4,334 events/s** sustained (1,670 s for the whole file) |
| Bronze rows == input events | 7,239,318 == 7,239,318 |
| Silver rows == distinct transactions, no duplicates | 7,203,302 == 7,203,302 |
| Late events (beyond the 10 s watermark) kept in silver | 50,896 |
| Alerts | 401 |
| Precision (alerts on an account with labelled fraud) | **83.3%** (82.3% counting only the scenario the rule targets) |

Recall counts incidents that start after a 30-minute warm-up and are wholly inside the file
(enough events to show the pattern):

| Scenario | Rule | Detected |
| --- | --- | --- |
| Velocity burst | `VELOCITY` | 65 / 65 |
| Account takeover | `NEW_DEVICE_CASH_OUT` | 48 / 59 |
| Mule ring | `MULE_FAN_IN` | 10 / 10 |
| Agent float spike | `AGENT_FLOAT_SPIKE` | 9 / 9 |
| PIN brute force | analyst rule `failed-pin-burst` | 20 / 20 |
| Device farm | analyst rule `device-farm` | 17 / 17 |
| Structuring | analyst rule `cash-in-structuring` | 1 / 1 |

Nine of the eleven missed takeovers had no payment in the two hours of history, so the rule rightly
treated them as unknown accounts (it alerts only when an account has a device older than an hour).
Production reads 90 days. The 67 alerts on accounts without labelled fraud are 33 new-device
cash-outs (legitimate customers on a phone the two hours of history had not seen), 24 velocity alerts
on heavy legitimate users and 10 agent spikes.

### What the runs found

Each run replays the same kind of traffic through the real job graph. Every one of them found
something the unit tests had not.

| Run | Change before the run | Events | Throughput | Alerts | Precision | Takeovers | Structuring |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [1](benchmarks/results/replay-20261021T212317Z.json) | baseline, 1M users, no history | 3,617,317 | 3,560/s | 5,714 | 2.1% | 0 / 57 | 0 / 3 |
| [2](benchmarks/results/replay-20261021T214300Z.json) | online features: one state value per account | 3,617,317 | 5,743/s | 5,753 | 2.1% | 0 / 57 | 0 / 3 |
| [3](benchmarks/results/replay-20261021T224458Z.json) | generator activity cap; 5M users; device history bootstrapped | 7,239,318 | 4,168/s | 633 | 52.9% | 48 / 59 | 0 / 1 |
| [4](benchmarks/results/replay-20261021T232835Z.json) | agent spike rule on the exact Poisson tail | 7,239,318 | 4,411/s | 403 | 83.1% | 48 / 59 | 0 / 1 |
| [5](benchmarks/results/replay-20261022T003309Z.json) | analyst rules independent of arrival order | 7,239,318 | 4,334/s | 401 | 83.3% | 48 / 59 | 1 / 1 |

1. **State layout decided throughput.** On RocksDB every state access is a serialised lookup.
   `OnlineFeatures` kept four separate states per account (minute buckets, hour buckets, a clock and
   a flush flag) and dominated the job. One POJO of sorted `long[]` rows per account, with the same
   output, gave +61% on the same data. Throughput then fell from run 2 to run 3 because the input
   changed: twice the events over five times the accounts, and a savepoint to restore.
2. **The simulator was the bug.** 5,483 of the first run's alerts were `VELOCITY`, mostly one
   simulated customer with 9,842 payments in 53 minutes: an uncapped Pareto draw over a million
   users. No wallet behaves like that. User activity is now capped at 20 times the mean, with a test
   that no account dominates.
3. **A fresh deployment is blind to takeovers.** From empty state the new-device rule caught none of
   57 takeovers: it needs to know each account's usual phone. The bootstrap writes that history from
   the lakehouse into the savepoint the job starts from (ADR 13).
4. **Four sigma is not rare at 20,000 agents.** The agent spike rule used mean + 4 sigma, which for
   an agent averaging 11 cash-outs in 10 minutes alerts at 24. Tested on every agent on every
   cash-out, that raised about 160 false alerts an hour. It now alerts when the exact Poisson tail
   is below one in a million (31 for that agent).
5. **Out-of-order events hid structuring.** Three agents' cash-ins into one wallet arrive through
   different partitions with different lag. When the middle one arrived after the last, the window
   ending at the late event held two and the window that now held three was never checked again.
   It only showed at parallelism 2 or more. Each arrival is now checked against every window it
   falls in, in Flink and in the Python backtester, with
   [conformance vector 07](rules/conformance/07_out_of_order.json) pinning both.

Runs vary slightly between repeats (late-row counts and a few alerts) because arrival into the
parallel operators is not deterministic.

### Decision API

| Run | Machine | Target rate | Achieved | Client p50 / p95 / p99 | Server p50 / p95 / p99 | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| [2026-10-21](benchmarks/results/decision-api-1000rps-devcontainer.json) | 4-vCPU shared cloud dev container; 2 uvicorn workers, local Redis 7 and the load client on the same box | 1,000/s for 60 s | 999/s | 1.8 / 5.3 / 48.0 ms | 0.7 / 3.1 / 25.7 ms | 0 |

Redis was filled with synthetic feature values of realistic size for the generator's 200,000
accounts, since this container has no Kafka or Flink; outcomes from that run are not meaningful,
latency is. p99 varied between 15 and 48 ms across 1,000/s runs on this shared machine, and the
two workers saturate at about 2,000/s here. The target is met at 1,000/s; the margin at p99 is thin
on this box, and the run on a dedicated machine with the full stack is the one to quote.
