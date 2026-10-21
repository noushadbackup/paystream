# PayStream design

## Goal

A data platform for a mobile financial service that a reviewer can run on a laptop and that answers
the questions a payments company actually asks of its data team:

1. Is anyone stealing money right now? (fraud alerts within seconds)
2. Did every taka that moved get recorded exactly once? (exactly-once lake, ledger reconciliation)
3. What happened yesterday, correctly? (gold models that include late data)
4. Can we fix history when something goes wrong? (reprocessing from bronze)

Non-goals: a payment app, model training (rules first; an ML scorer would read the same features),
multi-region, a production UI.

## Data

The generator simulates three populations (users, merchants, agents per district) with
Pareto-distributed activity, a contact graph for P2P, and log-normal amounts per transaction type,
capped at the 25,000 BDT personal limit. On top of normal traffic it injects:

| Imperfection | Default rate | Why it matters |
| --- | --- | --- |
| Late events | 1%, up to 30 min | phones offline; tests watermarks and the batch layer |
| Duplicates | 0.5%, same `txn_id` within 5 s | app retries; tests dedup |
| Failed payments | 2% | KPIs must separate attempts from money moved |
| Device changes | 0.2% | legitimate new phones: the false-positive source for the new-device rule |
| Schema v1 producers | on demand | old app versions without `channel` |

Fraud scenarios are labelled on a separate topic (`fraud.labels`) that the rules never read:

| Scenario | Shape | Rule that should catch it |
| --- | --- | --- |
| Velocity burst | 8-12 P2P transfers in under a minute | `VELOCITY` (≥ 6 in 60 s, keyed by sender) |
| Account takeover | new device, 18-25k BDT cash-out | `NEW_DEVICE_CASH_OUT` (≥ 15k from a device under 1 h old, account has an older device) |
| Mule ring | 12-20 victims pay one account within minutes, then it cashes out | `MULE_FAN_IN` (≥ 8 distinct senders in 10 min, keyed by receiver) |
| Agent float spike | 30-50 extra cash-outs at one agent in 10 min | `AGENT_FLOAT_SPIKE` (10-min count above mean + 4·sqrt(mean) of the agent's own last hour) |

## Processing

```
envelopes ─┬─ invalid ───────────────────────────────────────────▶ payments.dlq
           └─ valid ─┬───────────────────────────────────────────▶ bronze.payment_events (every copy)
                     └─ dedup(txn_id, TTL 2 h) ─┬────────────────▶ silver.payments
                                                ├─ 1-min windows ─▶ gold.kpi_minute
                                                │        └ late ──▶ silver.late_payments
                                                └─ success ─ rules ▶ fraud.alerts + silver.fraud_alerts
ledger WAL ─ Debezium ─ LedgerCdcJob (upsert) ───────────────────▶ silver.ledger_transfers / _entries
```

* **Watermarks:** bounded out-of-orderness of 10 s, per Kafka partition, idle partitions ignored
  after a minute.
* **Rules fire on arrival.** Waiting for the watermark would add 10 s to every alert. Windows are
  still in event time, so a late event counts against the moment it happened, and timers clean
  state once the watermark passes.
* **Two speeds of truth.** `gold.kpi_minute` is fast and excludes events that missed their window.
  `fct_daily_kpis` is built nightly from silver and includes them. The gap between the two is
  itself a metric (`late_count`).
* **Dedup twice.** Flink drops retries inside its state TTL; `stg_payments` drops anything older.

## Guarantees

See [ADR 3](adr/0003-delivery-guarantees.md). Money tables are exactly-once through checkpoint-aligned
Iceberg commits; alerts are at-least-once with deterministic ids for latency. Both are tested: the
end-to-end Flink test checks counts in every table, and the chaos run checks Kafka records == bronze
rows after SIGKILLs.

## Batch layer

dbt on Trino builds staging views and gold tables: daily KPIs, merchant revenue, agent liquidity
(float spent on cash-in, earned on cash-out), fraud alert statistics, and a stream-vs-ledger
reconciliation. A closed day that does not reconcile fails `dbt build`. Airflow runs it nightly,
then compacts Iceberg files and expires snapshots; a backfill DAG rebuilds chosen days of silver
from bronze.

## Tradeoffs

| Decision | Chosen | Over | ADR |
| --- | --- | --- | --- |
| Stream processor | Flink | Spark Structured Streaming | [1](adr/0001-flink-over-spark-streaming.md) |
| Table format | Iceberg | Delta, Hudi | [2](adr/0002-iceberg-lakehouse.md) |
| Delivery | exactly-once for money, at-least-once for alerts | one guarantee everywhere | [3](adr/0003-delivery-guarantees.md) |
| Ledger sync | Debezium CDC | dual writes | [4](adr/0004-cdc-over-dual-writes.md) |
| Money type | integer paisa | float, DECIMAL | [5](adr/0005-money-as-integer-paisa.md) |
| Kafka key | sender account | txn_id | [6](adr/0006-partition-by-account.md) |
| Schema Registry client | small REST client in the job | Confluent's Java client | keeps the jar on Maven Central only |

## Known limits

* Single Kafka broker and no JobManager HA: fine for a laptop, not for production.
* The new-device rule learns devices from the stream, so an account's first-ever transactions are
  never flagged. Production would seed device history from the identity service.
* Agent baselines need 20 minutes of history; spikes in a fresh run's first 20 minutes are missed.
* Rule thresholds are hand-set. Precision and recall from the benchmark are the evidence for them.
