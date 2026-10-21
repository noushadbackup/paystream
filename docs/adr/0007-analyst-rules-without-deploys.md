# 7. Analyst rules run without a deploy, behind maker-checker

**Status:** accepted

**Context.** Fraud patterns change weekly. If every threshold change is a Java change, a code review,
a jar and a Flink savepoint-restart, the fraud team waits days for engineering and the job restarts
for business reasons. Fraud teams at payment companies write and tune their own rules; what they need
from the platform is safety: no bad rule should take down the job, decline good customers at scale,
or go live without a second person and a record.

**Decision.**
* A rule is a JSON document with a small, closed vocabulary (filter, group_by, one aggregate over an
  event-time window, threshold, cooldown, action), contract in `rules/schema/rule.schema.json`.
* The rules service owns the lifecycle: versioned drafts, a backtest on lakehouse history, submit,
  approval by someone other than the author, then publication to a compacted Kafka topic. Each step
  writes a hash-chained audit row in the same transaction. A kill switch publishes a DISABLED
  version immediately.
* Flink joins the topic as broadcast state. A key function fans each payment out to the groupings
  live rules use (sender, receiver, device, district); a keyed evaluator keeps each key's matching
  history for the longest window and evaluates on arrival. State holds the published JSON, so the
  checkpoint format is the contract itself. Versions only move forward.
* New rules start in SHADOW: they alert with `shadow = true` and the console, case queue and
  decision service ignore them until someone promotes them.
* The four built-in detectors stay in Java. Their logic (per-agent baselines, device history) does
  not fit the vocabulary, and that is fine: the vocabulary is kept small on purpose.

**Why a second engine in Python.** A backtest is only worth something if it predicts production.
Both engines run the vectors in `rules/conformance` in CI; a semantic change has to land in both or
CI fails.

**Consequences and limits.**
* Evaluation cost per event is the number of matching events the key holds in its window. A rule
  grouping a broad filter by a low-cardinality field (district) creates hot keys. The backtest
  reports the largest per-key history, and the service refuses to approve a rule above 50,000.
* Rules and payments are separate streams, so a payment processed in the instant before a rule
  arrives is not evaluated by it. Rules apply from the moment they are live, not retroactively;
  the backtest is how history is covered.
* No sequences ("failed PIN then success"), joins, or ML features in the vocabulary yet. Sequences
  are the likely next addition; the ML score arrives through the decision service instead.
