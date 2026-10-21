# 11. AML reporting is built in the lakehouse, on the business day regulators use

**Status:** accepted

**Context.** A mobile financial service has two jobs the fraud stack does not cover: reporting cash
transactions above a threshold (CTR) and finding money-laundering patterns for suspicious-transaction
reports (STR) to the financial intelligence unit. These are batch questions over days, not
millisecond decisions, and they must be reproducible: a report for a past day has to come out the
same when it is rebuilt.

**Decision.**
* AML lives in dbt on the same silver tables as everything else, in its own `aml` schema, so it runs
  on Trino nightly and on DuckDB in CI, and a backfill of silver (ADR 2) rebuilds the reports too.
* The unit is the customer and the **business day in Asia/Dhaka** (UTC+6), not the UTC date: a
  cash-in at 20:00 UTC belongs to the next day's report.
* Thresholds are dbt vars (`ctr_threshold_bdt`, `per_txn_limit_bdt`, ...), set to the institution's
  reporting obligations, not literals in SQL. The CI values are illustrative.
* Typologies, each a lead, not a verdict: STRUCTURING (several cash-ins just under the single
  transaction limit at more than one agent in a day), PASS_THROUGH (most of a day's inflow leaves the
  same day), FAN_IN (many distinct senders in a day).
* `rpt_str_candidates` joins typology hits with fraud cases analysts confirmed (labels from ADR 9)
  into one worklist per customer for the money-laundering reporting officer, who decides what is filed.
* CTR report ids are derived from day, direction and customer, so a rebuilt report replaces the
  earlier one instead of duplicating it.

**Testing.** dbt unit tests pin each rule on hand-written rows (the Dhaka day boundary, thresholds,
each typology firing on its pattern and not on its look-alike); a mutation of the structuring rule
fails them. On the simulator, every structuring episode that is complete in the data must be found,
and the typologies flag 59 of the 61 simulated mule accounts in the CI fixture.

**Personal data.** Streams and tables carry account ids, not names or national ids; columns that
identify a customer are tagged `meta: {pii: true}` in the dbt docs. Anything meant to leave the AML
team (`share_*` models) replaces the id with a salted SHA-256 pseudonym, stable across days and not
reversible without the salt, which is a deployment secret passed as a dbt var. Restricting the `aml`
schema to the AML role is the query engine's job (Trino access control in a deployment); the
compose stack runs without authentication and does not enforce it.
