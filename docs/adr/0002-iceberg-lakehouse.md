# 2. Apache Iceberg for the lakehouse

**Status:** accepted

**Decision.** Iceberg tables (format v2) on S3-compatible storage (MinIO locally), served by a REST
catalog, written by Flink and read by Trino and dbt.

**Why.** It is engine-neutral: Flink writes, Trino and DuckDB read, Spark could join later without
a migration. Snapshots give atomic commits, which is what makes the exactly-once sink possible, and
time travel for audits. Upserts on a primary key (the ledger mirror) and partition evolution come
built in.

**Alternatives.** Delta Lake has the best Spark integration but weaker Flink and Trino write
support. Hudi is strong for upsert-heavy CDC but more complex to operate.

**Cost.** Streaming appends create many small files and snapshots; the nightly DAG compacts files,
expires snapshots and removes orphans.
