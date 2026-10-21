# 5. Money as integer paisa

**Status:** accepted

**Decision.** Every amount is a 64-bit integer number of paisa (1 BDT = 100 paisa): in Avro, in
Flink, in Postgres (`BIGINT` with `CHECK > 0`), in Iceberg and in dbt.

**Why.** Floats cannot represent most decimal amounts exactly, so sums drift. `DECIMAL` is exact but
slower to aggregate and easy to lose precision on when an engine casts it. Integers are exact, fast,
and the same in every engine; a signed 64-bit integer holds 92 quadrillion taka.

**Cost.** Display code divides by 100. Currencies with three decimal places would need a per-currency
scale.
