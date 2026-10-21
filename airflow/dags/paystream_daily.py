"""Nightly batch layer: freshness gate, gold models and reconciliation, then lakehouse maintenance.

The stream gives fast numbers; this DAG gives correct ones. It runs after midnight UTC, by which
time the previous day's late events (up to 30 min) and CDC changes have landed.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from paystream_common import DBT_CMD, DBT_ENV, STREAMING_TABLES, trino


@dag(
    schedule="0 2 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["paystream"],
)
def paystream_daily():
    freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"{DBT_CMD} source freshness",
        env=DBT_ENV,
    )

    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT_CMD} build",  # models + tests, including the reconciliation check
        env=DBT_ENV,
    )

    @task
    def compact_and_expire() -> None:
        statements = []
        for table in STREAMING_TABLES:
            statements += [
                f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '64MB')",
                f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '7d')",
                f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '7d')",
            ]
        trino(statements)

    freshness >> build >> compact_and_expire()


paystream_daily()
