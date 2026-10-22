#!/usr/bin/env bash
# Processing capacity of the Flink job without Kafka: generate traffic to a file, replay it through the
# production job graph as fast as it will go (RocksDB, exactly-once Iceberg commits, Redis), then
# score throughput, table correctness and fraud recall. Needs Java 17+, Maven, Python with the
# generator installed, and a Redis on REDIS_PORT (0 discards the online store writes).
#
# HISTORY_HOURS of earlier traffic are generated too and bootstrapped into a savepoint the replay
# starts from (DeviceHistoryBootstrap: known devices, and outgoing rates for the velocity rule), as a
# production deployment starts from the lakehouse's history; 0 replays from empty state.
#   RATE=2000 DURATION=1800 ./benchmarks/replay.sh
set -euo pipefail
cd "$(dirname "$0")/.."
RATE=${RATE:-2000}
DURATION=${DURATION:-1800}
USERS=${USERS:-5000000}
HISTORY_HOURS=${HISTORY_HOURS:-2}
SEED=${SEED:-42}
PARALLELISM=${PARALLELISM:-$(nproc)}
REDIS_PORT=${REDIS_PORT:-6379}
MANAGED_MEMORY=${MANAGED_MEMORY:-4g}   # RocksDB block cache and memtables, shared by all slots
WORK=${WORK:-benchmarks/replay-work}
START=2026-10-01T00:00:00+00:00
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
DATA="$WORK/data-r$RATE-d$DURATION-u$USERS-h$HISTORY_HOURS-s$SEED"
mkdir -p "$DATA" benchmarks/results
GEN=(--rate "$RATE" --users "$USERS" --merchants 20000 --agents 20000)

if [[ ! -s "$DATA/events.jsonl" ]]; then
  echo "== generating $RATE events/s for $DURATION simulated seconds, $USERS users"
  paystream-gen "${GEN[@]}" --duration "$DURATION" --seed "$SEED" --start "$START" \
    --out "$DATA/events.jsonl" --labels-out "$DATA/labels.jsonl"
fi
if [[ "$HISTORY_HOURS" -gt 0 && ! -s "$DATA/history.jsonl" ]]; then
  echo "== generating $HISTORY_HOURS h of earlier traffic"
  HSTART=$(python -c "from datetime import datetime, timedelta; print((datetime.fromisoformat('$START') - timedelta(hours=$HISTORY_HOURS)).isoformat())")
  paystream-gen "${GEN[@]}" --duration $((HISTORY_HOURS * 3600)) --seed $((SEED + 1)) --start "$HSTART" \
    --out "$DATA/history.jsonl"
fi

echo "== building"
(cd flink-jobs && mvn -q test-compile && mvn -q dependency:build-classpath -Dmdep.includeScope=test \
  -Dmdep.outputFile=target/test.classpath)
OPENS=""
for p in java.util java.lang java.nio sun.nio.ch java.lang.reflect java.net java.time; do
  OPENS="$OPENS --add-opens java.base/$p=ALL-UNNAMED"
done
CP="flink-jobs/target/test-classes:flink-jobs/target/classes:$(cat flink-jobs/target/test.classpath)"
RUN="$WORK/run-$STAMP"
mkdir -p "$RUN"
EXTRA=()
BOOT_S=0
if [[ "$HISTORY_HOURS" -gt 0 ]]; then
  echo "== bootstrapping device history into a savepoint"
  T0=$(date +%s)
  # shellcheck disable=SC2086
  java -Xmx4g $OPENS -cp "$CP" io.paystream.flink.bootstrap.DeviceHistoryBootstrap --jsonl "$DATA/history.jsonl" \
    --asOf "${START%+00:00}Z" --savepoint "file://$PWD/$RUN/savepoint" --maxParallelism 128 > "$RUN/bootstrap.log" 2>&1 \
    || { tail -50 "$RUN/bootstrap.log"; exit 1; }
  BOOT_S=$(( $(date +%s) - T0 ))
  EXTRA=(--fromSavepoint "file://$PWD/$RUN/savepoint")
fi

echo "== replaying with parallelism $PARALLELISM"
# shellcheck disable=SC2086
java -Xmx3g $OPENS -cp "$CP" io.paystream.flink.bench.ReplayBenchmark --events "$DATA/events.jsonl" --work "$RUN" \
  --rules rules/library --parallelism "$PARALLELISM" --maxParallelism 128 --redisPort "$REDIS_PORT" --managedMemory "$MANAGED_MEMORY" "${EXTRA[@]}" \
  > "$RUN/replay.log" 2>&1 || { tail -50 "$RUN/replay.log"; exit 1; }

OUT="benchmarks/results/replay-$STAMP.json"
python benchmarks/replay_report.py --events "$DATA/events.jsonl" --labels "$DATA/labels.jsonl" --run "$RUN" \
  --users "$USERS" --history-hours "$HISTORY_HOURS" --bootstrap-seconds "$BOOT_S" --note "${NOTE:-}" > "$OUT"
echo "== report: $OUT"
python -c "import json,sys; r=json.load(open(sys.argv[1])); q=r['quality']; print(r['throughput_events_per_s'], 'events/s;', r['correctness']); print({k: (v['detected'], v['incidents']) for k, v in q['recall_scored'].items()}, 'precision', q['precision'])" "$OUT"
