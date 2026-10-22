# 13. Stateful rules start warm: their history is bootstrapped from the lakehouse

**Status:** accepted

**Context.** Some detections depend on long memory. `NEW_DEVICE_CASH_OUT` asks whether an account
has paid from this phone before, over 90 days. That memory lives in Flink keyed state, so it is
empty whenever the job starts without a checkpoint: the first deployment, a migration to a new
cluster, a state schema change that cannot be restored, or recovery from a corrupted checkpoint.
The payments topic keeps three days, so replaying it cannot rebuild 90 days either. Until accounts have
been seen again on their usual phones (days, for most customers), takeovers go unflagged; the rule
deliberately stays silent on accounts it has no history for, or it would flag every customer on day
one.

The replay benchmark showed the size of the gap: from empty state the rule caught none of 41
simulated takeovers.

**Decision.** `DeviceHistoryBootstrap` builds the rule's state offline with Flink's State Processor
API and writes it as a savepoint; the payments job starts from that savepoint.

* Source: `silver.payments` for the last 90 days, the same rows the rule would have learned from
  (successful, customer-initiated, with a device), reduced to first sighting per account and device
  in SQL. A file source exists for benchmarks.
* The savepoint holds only the rule's operator (`uid` `rule-new-device`); every other operator starts
  empty, as on a fresh deployment. The state descriptor (name, types, 90-day TTL) is shared with the
  rule through `NewDeviceCashOutRule.historyDescriptor()`, so the two cannot drift apart.
* Max parallelism must equal the job's (480 in production): it fixes how keys map to key groups.
* An integration test bootstraps a savepoint, starts the rule from it and checks that a cash-out from
  an unseen phone alerts on the first payment, and that the same input from empty state does not.

**Consequences.** A cold start is a two-step deploy: run the bootstrap job, then start the payments
job with `-s <savepoint>`. The Kafka source then starts from its configured offsets, so the history
and the stream can overlap by the topic's retention; the rule keeps the earliest sighting, so the
overlap is harmless. Other rules have short memory (minutes to an hour) and warm up from the stream.

With two hours of bootstrapped history, the 60-minute replay caught 48 of 59 takeovers
([BENCHMARKS.md](../../BENCHMARKS.md)). Nine of the eleven misses had no payment at all in those two
hours, so the rule rightly treated them as unknown accounts; a 90-day history would cover most of them.
