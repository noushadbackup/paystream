#!/usr/bin/env bash
# The payments job end to end on real infrastructure without Docker: a Kafka broker (KRaft) and a
# standalone Flink cluster (JobManager + TaskManager) on this machine, the generator producing in
# real time, Redis for the online store, Iceberg tables in a local directory. Measures what the file
# replay cannot: alert latency from the payment's arrival in Kafka, consumer lag, checkpoint cost,
# silver freshness, and exactly-once across a TaskManager kill (CHAOS=1).
#
#   KAFKA_HOME=~/stack/kafka_2.13-3.8.1 FLINK_HOME=~/stack/flink-1.20.1 RATE=2000 DURATION=1800 \
#     HISTORY=benchmarks/replay-work/.../history.jsonl CHAOS=1 ./benchmarks/stack/run.sh
#
# Needs Java 17, Maven, Python with the generator ([kafka] extra), redis-server. HISTORY (optional)
# is earlier traffic in the generator's JSONL format, bootstrapped into the savepoint the job starts
# from (ADR 13). Schema Registry is replaced by benchmarks/stack/registry.py, which serves the same
# REST calls; clients cache schema ids, so it is not on the per-record path.
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${KAFKA_HOME:?set KAFKA_HOME to an unpacked Kafka 3.8}"
: "${FLINK_HOME:?set FLINK_HOME to an unpacked Flink 1.20}"
RATE=${RATE:-2000}
DURATION=${DURATION:-1800}
USERS=${USERS:-5000000}
SEED=${SEED:-42}
PARTITIONS=${PARTITIONS:-8}
PARALLELISM=${PARALLELISM:-4}
CHAOS=${CHAOS:-0}
REDIS_PORT=${REDIS_PORT:-6379}
TM_MEMORY=${TM_MEMORY:-6500m}
MANAGED_MEMORY=${MANAGED_MEMORY:-2500m}
HISTORY=${HISTORY:-}
# GEN_ARGS: extra generator flags, e.g. GEN_ARGS="--p-late 0" for no late events.
PY=${PY:-python}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN="$PWD/benchmarks/stack-work/run-$STAMP"
JAR="$PWD/flink-jobs/target/paystream-flink-0.1.0.jar"
BOOT=localhost:19092
REG=http://localhost:18081
mkdir -p "$RUN" benchmarks/results
log() { echo "== $(date -u +%H:%M:%S) $*"; }

PIDS=()
cleanup() {
  "$FLINK_HOME/bin/stop-cluster.sh" >/dev/null 2>&1 || true
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  [[ -n "${KAFKA_PID:-}" ]] && kill "$KAFKA_PID" 2>/dev/null || true
}
trap cleanup EXIT

log "building the job jar and the report classpath"
(cd flink-jobs && mvn -q package -DskipTests && mvn -q dependency:build-classpath -Dmdep.includeScope=test \
  -Dmdep.outputFile=target/test.classpath && mvn -q dependency:copy-dependencies -DincludeArtifactIds=hadoop-client-api,hadoop-client-runtime,commons-logging \
  -DincludeScope=provided -DoutputDirectory=target/flink-lib)

log "kafka (KRaft, one broker) with $PARTITIONS partitions for payments"
sed -e "s|^log.dirs=.*|log.dirs=$RUN/kafka|" \
    -e "s|^listeners=.*|listeners=PLAINTEXT://localhost:19092,CONTROLLER://localhost:19093|" \
    -e "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://localhost:19092|" \
    -e "s|^controller.quorum.voters=.*|controller.quorum.voters=1@localhost:19093|" \
    -e "s|^num.partitions=.*|num.partitions=1|" \
    "$KAFKA_HOME/config/kraft/server.properties" > "$RUN/server.properties"
"$KAFKA_HOME/bin/kafka-storage.sh" format -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" \
  -c "$RUN/server.properties" > /dev/null
start_kafka() {
  KAFKA_HEAP_OPTS="-Xms1g -Xmx1g" "$KAFKA_HOME/bin/kafka-server-start.sh" "$RUN/server.properties" \
    >> "$RUN/kafka.log" 2>&1 &
  KAFKA_PID=$!
  until "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server $BOOT --list >/dev/null 2>&1; do sleep 1; done
}
start_kafka
topic() { "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server $BOOT --create --topic "$1" --partitions "$2" "${@:3}" >/dev/null; }
topic payments.events "$PARTITIONS"
topic fraud.alerts 4
topic fraud.labels 1
topic fraud.rules 1 --config cleanup.policy=compact
topic payments.dlq 1
topic payments.decisions 1

log "schema registry stand-in"
$PY benchmarks/stack/registry.py --port 18081 > "$RUN/registry.log" 2>&1 &
PIDS+=($!)

log "analyst rules from rules/library"
$PY - "$BOOT" rules/library <<'EOF'
import json, pathlib, sys
from confluent_kafka import Producer
p = Producer({"bootstrap.servers": sys.argv[1]})
for f in sorted(pathlib.Path(sys.argv[2]).glob("*.json")):
    rule = json.loads(f.read_text())
    p.produce("fraud.rules", key=rule["rule_id"], value=json.dumps(rule))
p.flush(10)
EOF

if ! redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1; then
  log "redis on $REDIS_PORT"
  redis-server --port "$REDIS_PORT" --save "" --appendonly no --maxmemory 2500mb --maxmemory-policy allkeys-lru \
    > "$RUN/redis.log" 2>&1 &
  PIDS+=($!)
  until redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1; do sleep 1; done
fi
redis-cli -p "$REDIS_PORT" flushall >/dev/null

log "flink cluster: 1 JobManager, 1 TaskManager with $PARALLELISM slots, RocksDB, checkpoints every 10 s"
export FLINK_CONF_DIR="$RUN/flink-conf" FLINK_LOG_DIR="$RUN/flink-log" FLINK_LIB_DIR="$RUN/flink-lib"
mkdir -p "$FLINK_CONF_DIR" "$FLINK_LOG_DIR" "$FLINK_LIB_DIR"
# Flink's lib plus the Hadoop client, as the image has it (flink-jobs/Dockerfile)
ln -s "$FLINK_HOME"/lib/*.jar "$PWD"/flink-jobs/target/flink-lib/*.jar "$FLINK_LIB_DIR/"
cp "$FLINK_HOME"/conf/{log4j.properties,log4j-cli.properties,masters,workers} "$FLINK_CONF_DIR/"
JAVA_OPTS=$(grep -m1 -- '--add-exports' "$FLINK_HOME/conf/config.yaml" | sed 's/^ *all: //')
cat > "$FLINK_CONF_DIR/config.yaml" <<EOF
env.java.opts.all: $JAVA_OPTS
jobmanager.rpc.address: localhost
jobmanager.bind-host: localhost
taskmanager.bind-host: localhost
taskmanager.host: localhost
rest.address: localhost
rest.bind-address: localhost
rest.port: 18082
jobmanager.memory.process.size: 1200m
taskmanager.memory.process.size: $TM_MEMORY
taskmanager.memory.managed.size: $MANAGED_MEMORY
taskmanager.numberOfTaskSlots: $PARALLELISM
parallelism.default: $PARALLELISM
pipeline.max-parallelism: 128
state.backend.type: rocksdb
state.backend.incremental: true
execution.checkpointing.dir: file://$RUN/checkpoints
execution.checkpointing.savepoint-dir: file://$RUN/savepoints
restart-strategy.type: exponential-delay
EOF
# jemalloc for the Flink processes, as the official Flink image does: with glibc malloc, RocksDB's native
# memory fragments well past its managed budget (first run: 8.1 GB RSS for a 6.5 GB TaskManager)
JEMALLOC=${JEMALLOC-/usr/lib/x86_64-linux-gnu/libjemalloc.so.2}
[[ -f "$JEMALLOC" ]] && PRELOAD="$JEMALLOC" || PRELOAD=""
LD_PRELOAD="$PRELOAD" "$FLINK_HOME/bin/start-cluster.sh" > /dev/null
until curl -sf localhost:18082/taskmanagers | grep -q '"slotsNumber"'; do sleep 1; done

FROM=()
if [[ -n "$HISTORY" ]]; then
  log "bootstrapping device history from $HISTORY into a savepoint"
  T0=$(date +%s)
  "$FLINK_HOME/bin/flink" run -c io.paystream.flink.bootstrap.DeviceHistoryBootstrap "$JAR" \
    --jsonl "$(realpath "$HISTORY")" --savepoint "file://$RUN/bootstrap" --maxParallelism 128 > "$RUN/bootstrap.log" 2>&1
  echo "$(( $(date +%s) - T0 ))" > "$RUN/bootstrap_seconds"
  FROM=(-s "file://$RUN/bootstrap")
fi

log "submitting the payments job"
"$FLINK_HOME/bin/flink" run -d "${FROM[@]}" -c io.paystream.flink.PaymentsJob "$JAR" \
  --bootstrap $BOOT --registry $REG --catalogType hadoop --warehouse "file://$RUN/warehouse" \
  --redisHost localhost --redisPort "$REDIS_PORT" > "$RUN/submit.log" 2>&1
JOB=$(grep -o 'JobID [0-9a-f]*' "$RUN/submit.log" | awk '{print $2}')
until curl -sf "localhost:18082/jobs/$JOB" | grep -q '"state":"RUNNING"'; do sleep 1; done
sleep 20  # sources assigned, Iceberg tables created

log "sampling metrics; generating $RATE payments/s for $DURATION s in real time ($USERS users)"
$PY benchmarks/stack/report.py sample --job "$JOB" --bootstrap $BOOT --out "$RUN/samples.jsonl" \
  > "$RUN/sampler.log" 2>&1 &
SAMPLER=$!
PIDS+=($SAMPLER)
paystream-gen --sink kafka --bootstrap $BOOT --registry $REG --rate "$RATE" --duration "$DURATION" --realtime \
  --users "$USERS" --merchants 20000 --agents 20000 --seed "$SEED" ${GEN_ARGS:-} > "$RUN/generator.log" 2>&1 &
GEN=$!
# Late events and the tails of fraud scenarios keep arriving for up to 30 min; stop after TAIL seconds
# more (most late events come within a few minutes). Whatever was produced is what gets checked.
( sleep $((DURATION + ${TAIL:-180})); kill -TERM "$GEN" 2>/dev/null ) &
PIDS+=($!)

if [[ "$CHAOS" == "1" ]]; then
  for at in $((DURATION / 3)) $((2 * DURATION / 3)); do
    sleep $(( at - ${slept:-0} )); slept=$at
    TM=$(pgrep -f 'org.apache.flink.runtime.taskexecutor.TaskManagerRunner' | head -1)
    log "chaos: kill -9 TaskManager $TM"
    echo "{\"event\": \"kill_taskmanager\", \"at\": $(date +%s)}" >> "$RUN/chaos.jsonl"
    kill -9 "$TM"
    sleep 10
    LD_PRELOAD="$PRELOAD" "$FLINK_HOME/bin/taskmanager.sh" start > /dev/null
  done
fi
wait "$GEN"

log "generator done; waiting for the job to drain"
$PY benchmarks/stack/report.py drain --job "$JOB" --timeout 900
log "stopping with a final savepoint (commits the last Iceberg files)"
"$FLINK_HOME/bin/flink" stop "$JOB" > "$RUN/stop.log" 2>&1 \
  || log "stop-with-savepoint failed (stop.log); the drained data was committed by the checkpoints before it"
kill "$SAMPLER" 2>/dev/null || true

log "reading the lakehouse"
CP="flink-jobs/target/test-classes:flink-jobs/target/classes:$(cat flink-jobs/target/test.classpath)"
java -Xmx3g $JAVA_OPTS -cp "$CP" io.paystream.flink.bench.LakeReport --warehouse "file://$RUN/warehouse" \
  --work "$RUN" --freshness > "$RUN/lake-report.log" 2>&1 || { tail -30 "$RUN/lake-report.log"; exit 1; }

OUT="benchmarks/results/stack-$STAMP.json"
$PY benchmarks/stack/report.py final --run "$RUN" --bootstrap $BOOT --rate "$RATE" \
  --duration "$DURATION" --users "$USERS" --partitions "$PARTITIONS" --parallelism "$PARALLELISM" \
  --chaos "$CHAOS" --warmup-minutes "${WARMUP_MINUTES:-30}" --note "${NOTE:-}" > "$OUT"
log "report: $OUT"
$PY -c "import json,sys; r=json.load(open(sys.argv[1])); print(json.dumps({k: r[k] for k in ('throughput','alert_latency_ms','silver_freshness_ms','exactly_once')}, indent=1))" "$OUT"
