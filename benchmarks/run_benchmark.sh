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
{ uname -sm; nproc; free -g 2>/dev/null | awk '/Mem/{print $2 " GB RAM"}' || sysctl -n hw.memsize; } | tee "$RUN_DIR/$STAMP.machine.txt"

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
