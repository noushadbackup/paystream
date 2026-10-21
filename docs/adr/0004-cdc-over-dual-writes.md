# 4. CDC from the ledger instead of dual writes

**Status:** accepted

**Decision.** The ledger (Postgres) is the system of record. Debezium reads its WAL and streams
changes to Kafka; `LedgerCdcJob` upserts them into Iceberg.

**Why.** An application that writes to the database and then to Kafka will eventually do one and
not the other (a crash between the two, a timeout on the second). Reading the WAL means the lake
sees exactly what committed, in commit order, without touching the application.

**What it enables.** The gateway stream and the ledger are two independent views of the same money.
`recon_stream_vs_ledger` compares them every day; a closed day that does not match fails the build.
