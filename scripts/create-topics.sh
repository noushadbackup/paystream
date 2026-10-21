#!/usr/bin/env bash
# Topics are created explicitly (auto-create is off) so partition counts are a decision, not an accident.
# payments.events: 12 partitions keyed by sender, enough for 12-way Flink parallelism.
set -euo pipefail
K=/opt/kafka/bin/kafka-topics.sh
B=kafka:9092
create() { $K --bootstrap-server $B --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1 ${3:-}; }
create payments.events 12 "--config retention.ms=259200000 --config compression.type=lz4"
create fraud.alerts 6
create fraud.labels 3
create payments.dlq 3 "--config retention.ms=1209600000"
# every decision with the features it saw: audit trail and model training data
create payments.decisions 6 "--config retention.ms=604800000 --config compression.type=lz4"
# analyst rules: one record per rule_id (latest version wins), read in full by every Flink restart
create fraud.rules 1 "--config cleanup.policy=compact --config min.compaction.lag.ms=0"
$K --bootstrap-server $B --list
