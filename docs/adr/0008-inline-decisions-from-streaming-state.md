# 8. Inline decisions read state that Flink keeps in Redis

**Status:** accepted

**Context.** Alerts after the fact are not enough for money that leaves in seconds: a cash-out from a
taken-over account is gone before an analyst opens the alert. The gateway needs an answer before it
authorises (approve, step-up authentication, hold for review, or decline) inside its own latency
budget, about 50 ms for the risk check.

**Decision.**
* Flink maintains the online state as a side output of the same job that does detection: per sender,
  receiver and device, minute buckets for the last hour, hour buckets for the last day and recent
  counterparties; the devices each account has used; and risk flags from live (non-shadow) alerts.
  It writes them to Redis, coalesced to at most one write per key every 200 ms, pipelined.
* The decision service (Python, FastAPI) reads everything for a payment in one pipelined Redis round
  trip, computes the feature windows at decision time, and applies a policy that returns an outcome
  and reason codes. Every decision goes to Kafka with the exact features it used, and from there to
  `silver.decisions`.
* If Redis does not answer within 25 ms, the service still answers: limits are checked, small
  payments are approved, larger ones held for review. Degraded decisions are counted and logged.

**Why buckets, not finished numbers.** A count written by Flink is true only when written. Buckets
let the reader evaluate "last 10 minutes" at the moment of the decision, so quiet accounts age out
without writes. Rewriting the whole bucket set is idempotent, which makes the at-least-once Redis
sink safe under replay.

**Why log served features.** Recomputing features later from the lake for model training gives
slightly different values (late events, window edges) than the model saw online. Training on the
logged features avoids that skew; labels are joined later.

**Consequences.**
* Streaming detection and inline decisions are one loop: a DECLINE rule fires in Flink and the
  account's next payment is declined, typically within a second.
* Features are up to ~200 ms plus pipeline latency stale. A burst within that gap is caught by the
  next payment or by the streaming rules.
* Redis holds derived state only. Losing it loses nothing: Flink restores from its checkpoint and
  rewrites every active key as traffic arrives. Until then decisions run degraded, by design.
