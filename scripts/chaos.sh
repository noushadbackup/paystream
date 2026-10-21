#!/usr/bin/env bash
# Failure injection for the exactly-once claim. Processes get SIGKILL (no graceful shutdown), so a
# kill often lands mid-checkpoint. Afterwards `python benchmarks/measure.py --verify-only` must report
#   no_loss_no_dup_in_bronze = true   (every Kafka record is in bronze exactly once)
#   silver_is_deduplicated   = true
set -euo pipefail
cd "$(dirname "$0")/.."
log() { echo "$(date -u +%T) chaos: $*"; }

log "SIGKILL taskmanager (all operator state lost from memory)"
docker compose kill -s SIGKILL taskmanager
sleep 20
docker compose up -d taskmanager
log "taskmanager back; job restores from the last completed checkpoint"

sleep 120
log "SIGKILL kafka broker (producers and the Flink source must ride it out)"
docker compose kill -s SIGKILL kafka
sleep 15
docker compose up -d kafka
log "kafka back"

sleep 120
log "SIGKILL taskmanager again, now with a backlog to catch up on"
docker compose kill -s SIGKILL taskmanager
sleep 20
docker compose up -d taskmanager
log "done"
