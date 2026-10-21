# 6. Kafka key = sending account

**Status:** accepted

**Decision.** `payments.events` is keyed by `sender_id` (12 partitions).

**Why.** One account's payments stay in order within a partition, which keeps the velocity and
new-device rules simple and lets a consumer process an account's history in order. Rules keyed by
receiver (mule fan-in, agent spikes) re-key inside Flink, which costs a network shuffle but no
ordering guarantee is needed there because they work in event time.

**Risk.** Hot keys: a very active account or merchant concentrates load on one partition. The
generator's activity is Pareto-distributed on purpose so this shows up in benchmarks; per-partition
lag in Grafana is how it would be spotted.
