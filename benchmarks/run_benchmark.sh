#!/usr/bin/env bash
# One benchmark run: drive load at RATE events/s for DURATION seconds and measure from the outside.
#   RATE=5000 DURATION=1800 ./benchmarks/run_benchmark.sh
#   CHAOS=1 RATE=5000 DURATION=1800 ./benchmarks/run_benchmark.sh   # also kill the TaskManager mid-run
set -euo pipefail
cd "$(dirname "$0")/.."
RATE=${RATE:-5000}
DURATION=${DURATION:-1800}
SEED=${SEED:-42}
RUN_DIR=benchmarks/runs
mkdir -p "$RUN_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)

echo "== machine"
# Host RAM has no portable command (free on Linux, sysctl on macOS, neither in Git Bash), so also
# record what Docker itself is allowed to use: on Docker Desktop that is the real ceiling.
{ uname -sm; nproc
  if command -v free >/dev/null; then free -g | awk '/Mem/{print $2 " GB RAM"}'
  elif command -v sysctl >/dev/null; then echo "$(( $(sysctl -n hw.memsize) / 1073741824 )) GB RAM"; fi
  docker info --format 'docker: {{.NCPU}} CPUs, {{.MemTotal}} bytes memory, {{.OperatingSystem}}'
} | tee "$RUN_DIR/$STAMP.machine.txt"

echo "== load: $RATE events/s for $DURATION s (seed $SEED)"
docker compose --profile load run -d --rm --name paystream-load generator \
  --sink kafka --bootstrap kafka:9092 --registry http://schema-registry:8081 \
  --ledger-dsn postgresql://paystream:paystream@postgres:5432/paystream \
  --realtime --rate "$RATE" --duration "$DURATION" --seed "$SEED" --users 1000000 --agents 20000

if [[ "${CHAOS:-0}" == "1" ]]; then
  ( sleep $((DURATION / 3)); ./scripts/chaos.sh ) &
fi

python benchmarks/measure.py --duration "$DURATION" --out "$RUN_DIR/$STAMP.json"
echo "== report: $RUN_DIR/$STAMP.json"

# The exactly-once count in that report is taken while the generator is still delivering late events
# (1% arrive up to 30 minutes after they happened), so Kafka is always a little ahead of bronze there.
# The real check runs once the generator has exited and two checkpoints have committed the tail.
echo "== waiting for late events to drain (generator exits up to 30 min after the run)"
docker wait paystream-load >/dev/null 2>&1 || true
sleep 30
python benchmarks/measure.py --verify-only --out "$RUN_DIR/$STAMP.verify.json"
echo "== verify: $RUN_DIR/$STAMP.verify.json"
