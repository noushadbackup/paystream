# PayStream: common tasks. `make help` lists them.
.DEFAULT_GOAL := help
COMPOSE := docker compose
JAR := /opt/paystream/paystream-flink.jar
PY ?= python3

help: ## List targets
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

up: ## Start the core stack and submit both Flink jobs
	$(COMPOSE) up -d --build --wait kafka schema-registry postgres connect minio iceberg-rest jobmanager taskmanager trino prometheus grafana kafka-exporter
	$(COMPOSE) up kafka-init connect-init
	$(MAKE) submit
	@echo "Flink UI http://localhost:8082 · Grafana http://localhost:3000 · Trino localhost:8080 · MinIO http://localhost:9001"

up-batch: ## Also start Airflow (http://localhost:8088)
	$(COMPOSE) --profile batch up -d --build airflow

submit: ## Submit the payments job and the ledger CDC job
	$(COMPOSE) exec jobmanager flink run -d -c io.paystream.flink.PaymentsJob $(JAR)
	$(COMPOSE) exec jobmanager flink run -d -c io.paystream.flink.LedgerCdcJob $(JAR)

load: ## Generate 1,000 events/s for 10 minutes (into Kafka and the ledger)
	$(COMPOSE) --profile load run --rm generator --sink kafka --bootstrap kafka:9092 \
	  --registry http://schema-registry:8081 --ledger-dsn postgresql://paystream:paystream@postgres:5432/paystream \
	  --realtime --rate 1000 --duration 600

bench: ## Benchmark run (RATE, DURATION env vars; CHAOS=1 adds failure injection)
	./benchmarks/run_benchmark.sh

chaos: ## Kill the TaskManager and the broker mid-stream
	./scripts/chaos.sh

verify: ## Exactly-once check: Kafka == bronze, silver deduplicated, fraud recall
	$(PY) benchmarks/measure.py --verify-only

sql: ## Trino shell on the lakehouse
	$(COMPOSE) exec trino trino --catalog lake

dbt: ## Build gold models and run data tests on Trino
	cd dbt && dbt build --profiles-dir .

schema-v1: ## Produce with the old schema (v1) to show evolution: the job keeps running, channel is null
	$(COMPOSE) --profile load run --rm generator --sink kafka --bootstrap kafka:9092 \
	  --registry http://schema-registry:8081 --schema-version 1 --rate 200 --duration 60 --realtime

test: ## All tests: generator, Flink (incl. Iceberg end-to-end), dbt on DuckDB
	cd generator && $(PY) -m pytest -q
	cd flink-jobs && mvn -q -B test
	cd dbt && $(PY) ci/build_fixture.py && dbt build --profiles-dir . --target duckdb
	$(PY) -m pytest -q dashboard/tests

console: ## Fraud Ops Console on the live stack (http://localhost:8501)
	$(COMPOSE) up -d --build ops-console

demo: ## Fraud Ops Console on generated demo data, no Docker needed
	pip install -q -e generator "dbt-core>=1.10,<2" dbt-duckdb -r dashboard/requirements.txt
	cd dbt && $(PY) ci/build_fixture.py && dbt build --profiles-dir . --target duckdb -q
	cd dashboard && PAYSTREAM_DUCKDB=../dbt/ci/ci.duckdb streamlit run app.py

down: ## Stop everything and delete data volumes
	$(COMPOSE) --profile batch --profile load down -v

.PHONY: help up up-batch submit load bench chaos verify sql dbt schema-v1 test console demo down
