# 10. The fraud model trains on the decision log and is served as data

**Status:** accepted

**Context.** Rules catch the patterns someone has already described. A model can weigh the same
signals together, but two things usually go wrong in fraud models. Training features are rebuilt
offline by different code than the one that serves them, so the model learns numbers it never sees
in production (training-serving skew). And a new model goes live because it scored well on a
random split of old data, which leaks bursts across the split and says nothing about today.

**Decision.**
* **Training data is the decision log.** Every decision already records the exact features it was
  made on (`silver.decisions`). Labels are joined by `txn_id`: FRAUD from cases and the simulator,
  minus anything an analyst cleared as NOT_FRAUD. Decisions younger than the label maturity window
  (7 days on the stack) are left out, so "no label yet" is not taught as "legitimate". Nothing is
  recomputed, so there is nothing to skew.
* **Validation is out of time** (train on the earlier part of the window, validate on the later
  part), and results are reported at a review budget: the share of payments the model may hold.
  The threshold is the score at that budget on validation traffic, chosen without labels.
* **Serving needs no ML library.** LightGBM is exported to a flat JSON tree format that the decision
  service evaluates in pure Python (about 120 µs per payment, inside a 25 ms store budget). The
  export is checked against LightGBM's own prediction, raw scores equal to 1e-9, with both NaN and
  zero-as-missing handling, in CI and in every model card.
* **Champion and challenger.** A registry directory holds versions and two pointers. The decision
  service re-reads them every 30 s: the champion's score can hold a payment for review
  (`MODEL_SCORE`), the challenger is scored on the same payment and only logged. Both scores land in
  `silver.decisions`, so the comparison is on identical traffic.
* **Promotion is a gate, not a judgement call.** Daily, the challenger is compared with the champion
  on decisions made after the challenger's training window. It is promoted only if a paired
  bootstrap of the difference in average precision does not show it credibly worse (5th percentile
  above -0.01) and it keeps recall at the budget within a point. Not enough fresh labels means wait,
  not guess. Every promotion, refusal and rollback is appended to `history.jsonl`.
* **Drift.** Daily population stability index of every feature and of the score against the
  distributions in the champion's card; above 0.25 the Airflow task fails.

**Consequences.**
* A bad model artifact cannot take down payments: the registry keeps serving the previous one.
* Without a live stack, `paystream-ml replay` produces the decision log by running generated traffic
  through the decision service's own feature and policy code, with an in-memory stand-in for Flink's
  buckets and a 1 s visibility lag. CI trains, promotes and checks drift on that log. Numbers from
  it describe the simulator's fraud, not real-world performance.
* The model only sees decision-time features. Graph features (mule networks) and per-merchant
  baselines would need more online state in Flink first.
