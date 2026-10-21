# 1. Flink over Spark Structured Streaming

**Status:** accepted

**Context.** Fraud alerts must land within about 2 seconds of the gateway receiving a payment, and
the rules need per-account state (recent transfers, known devices, an agent's hourly baseline).

**Decision.** Apache Flink, DataStream API in Java, with keyed state on RocksDB.

**Why.** Flink processes each event as it arrives, so the latency floor is milliseconds, not a
micro-batch trigger interval. Keyed state with timers and TTL maps directly onto rules like "count
this account's transfers in the last 60 s". Event time, watermarks and side outputs for late data
are first-class.

**Cost.** A heavier operational surface than Spark (JobManager, TaskManagers, savepoints), and a
smaller hiring pool. Spark would be the better call for a batch-first team that only needed
minute-level freshness.
