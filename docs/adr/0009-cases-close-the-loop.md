# 9. Cases close the loop: verdicts become labels and account actions

**Status:** accepted

**Context.** Detection produces alerts; the decision service holds payments for review. Without a
place where people decide, alerts pile up, the same customer is reviewed five times for one burst,
and nothing learns from the decisions: rule precision is measured against simulator labels only, and
a model has no ground truth from production. A wrongly flagged customer stays blocked until a flag
happens to expire.

**Decision.**
* A case service (Python, FastAPI, Postgres) consumes `fraud.alerts` and the REVIEW outcomes in
  `payments.decisions`. There is one open case per subject (account or device); new alerts join it.
  Priority comes from the pattern and the rule's action and sets the SLA: P1 15 minutes (money can
  leave now), P2 2 hours, P3 24 hours. A more urgent alert escalates an open case and tightens its
  SLA, never loosens it. Shadow alerts never become work.
* Analysts claim the next case (`FOR UPDATE SKIP LOCKED`, so two people never get the same one),
  add notes, and close with CONFIRMED_FRAUD, FALSE_POSITIVE or INCONCLUSIVE.
* A verdict labels every alert on the case (`fraud.labels`, the same Avro contract the simulator
  uses, now v2 with `source`, `disposition`, `case_id`, `labelled_by`), so dbt measures rule
  precision against analyst decisions and models train on them.
* CONFIRMED_FRAUD freezes the subject; the decision service declines its next payment. FALSE_POSITIVE
  lifts the flags the case's rules raised and allows those rules on that subject for 72 hours, so the
  stream re-flagging the same customer does not block them again; the same rule on the same subject
  is also suppressed from the queue for 72 hours. Freezes are lifted by a lead, not an analyst.

**Effects go through an outbox.** Closing a case writes the verdict and the effects it implies in
one database transaction. A dispatcher then publishes labels and writes Redis, in order, stopping at
the first failure and retrying it later. The API dispatches immediately, so a freeze is live within
the request; the consumer retries anything that failed. A crash between the commit and Kafka delays
an effect but cannot lose it, and every effect is idempotent, so applying one twice is harmless.

**Analyst actions are not stream flags.** Flink writes `flags:{id}` with a short TTL that each new
alert refreshes. Analyst actions live in `case:{id}`, which the decision service reads in the same
pipelined round trip. Keeping them apart means a stream write can never shorten a freeze, and a
cleared customer is protected by an `ALLOW:{rule}` entry rather than by racing Flink to delete flags.

**Postgres is the record, Redis is a cache.** Redis runs without persistence and may evict. Every
freeze and clearance in force can be rebuilt from the case tables (`paystream-cases resync`); the
consumer does it on start and every five minutes. Expiry is computed from when the case closed, so a
retry or a rebuild never extends a freeze.

**Consequences.**
* Labels now carry a disposition. Precision in `fct_rule_performance_daily` counts a FRAUD label as a
  hit and a NOT_FRAUD label as a known false alarm; unlabelled alerts stay unlabelled, not negatives.
* Label schema v2 is BACKWARD compatible: v1 producers (the simulator before this change) keep
  working, and the Iceberg table gains the columns through an additive migration.
* A freeze here is a risk decision, not a ledger state: the core banking system remains the owner of
  account status. In a deployment the freeze would also call that system; here the decision service
  is the enforcement point.
