# 3. Exactly-once for money, at-least-once for alerts

**Status:** accepted

**Context.** A transactional (exactly-once) Kafka sink only makes records visible to
`read_committed` consumers when the Flink checkpoint completes, every 10 s here.

**Decision.**
* Iceberg tables (bronze, silver, gold, ledger mirror): exactly-once. Files are committed as one
  snapshot per checkpoint; after a failure the job rewinds to the last checkpoint and the
  uncommitted files are never referenced.
* `fraud.alerts`: at-least-once, with a deterministic `alert_id` (`rule:account:trigger_txn`).
  Consumers and `stg_fraud_alerts` deduplicate on it.

**Why.** Exactly-once on alerts would add up to a checkpoint interval (10 s) of latency to every
alert, which breaks the 2 s target. A repeated alert is harmless when its id is stable; a missing or
double-counted taka in the ledger is not.

**How it is verified.** `scripts/chaos.sh` SIGKILLs the TaskManager and the broker mid-run;
`measure.py --verify-only` then checks Kafka records == bronze rows and silver rows == distinct
transaction ids.
