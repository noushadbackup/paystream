# PayStream: common tasks. `make help` lists them.
.DEFAULT_GOAL := help
COMPOSE := docker compose
JAR := /opt/paystream/paystream-flink.jar
PY ?= python3

help: ## List targets
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

up: ## Start the core stack and submit both Flink jobs
	$(COMPOSE) up -d --build --wait kafka schema-registry postgres connect minio iceberg-rest jobmanager taskmanager trino prometheus grafana kafka-exporter redis
	$(COMPOSE) up kafka-init connect-init
	$(COMPOSE) up -d --build --wait rules-service decision-service case-service case-consumer
	$(MAKE) submit
	@echo "Flink UI http://localhost:8082 · Grafana http://localhost:3000 · Trino localhost:8080 · MinIO http://localhost:9001"
	@echo "Rules API http://localhost:8090/docs · Decision API http://localhost:8095/docs · Case API http://localhost:8097/docs"

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

rules-demo: ## Write, backtest, approve and publish a rule through the rules service; Flink picks it up live
	./scripts/rules-demo.sh

decide-load: ## Load-test the decision API (RPS, DURATION); run `make load` alongside so features are live
	pip install -q -e generator -e "decision-service[loadtest]"
	paystream-decision-loadtest --url http://localhost:8095 --rps $${RPS:-1000} --seconds $${DURATION:-60} \
	  --out benchmarks/runs/decision-$$(date +%s).json

model: ## Train a challenger on the lakehouse and promote it if it beats the champion (needs `make up-batch`)
	$(COMPOSE) --profile batch exec airflow sh -c '$$ML_BIN train --trino trino --registry /models --days 35 \
	  && $$ML_BIN promote --trino trino --registry /models'

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
	cd rules-service && $(PY) -m pytest -q
	cd decision-service && $(PY) -m pytest -q
	cd case-service && $(PY) -m pytest -q
	cd ml && $(PY) -m pytest -q
	cd flink-jobs && mvn -q -B verify
	cd dbt && $(PY) ci/build_fixture.py && paystream-ml replay --duckdb ci/ci.duckdb \
	  && dbt build --profiles-dir . --target duckdb
	$(PY) -m pytest -q dashboard/tests

console: ## Fraud Ops Console on the live stack (http://localhost:8501)
	$(COMPOSE) up -d --build ops-console

demo: ## Fraud Ops Console on generated demo data, no Docker needed
	pip install -q -e generator -e "rules-service[dev]" -e case-service -e decision-service -e ml \
	  "dbt-core>=1.10,<2" dbt-duckdb -r dashboard/requirements.txt
	cd dbt && $(PY) ci/build_fixture.py && paystream-ml replay --duckdb ci/ci.duckdb && dbt build --profiles-dir . --target duckdb -q
	cd dashboard && PAYSTREAM_DUCKDB=../dbt/ci/ci.duckdb streamlit run app.py

down: ## Stop everything and delete data volumes
	$(COMPOSE) --profile batch --profile load down -v

.PHONY: help up up-batch submit load bench chaos verify sql dbt schema-v1 test console demo down rules-demo decide-load
