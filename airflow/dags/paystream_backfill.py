"""Reprocess closed days: rebuild silver from bronze, then rebuild gold.

Bronze keeps every record exactly as it arrived, duplicates included, so silver can always be
recomputed from it: after a dedup bug, a rule change, or a lost partition. The rebuild is a
delete-then-insert of one day's partition in a single DAG task, so rerunning it is safe.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from paystream_common import DBT_CMD, DBT_ENV, sql, trino


@dag(
    schedule=None,
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    params={
        "start": Param(str(datetime.now(UTC).date() - timedelta(days=2)), type="string", format="date"),
        "end": Param(str(datetime.now(UTC).date() - timedelta(days=2)), type="string", format="date"),
    },
    tags=["paystream", "backfill"],
)
def paystream_backfill():
    @task
    def days(**ctx) -> list[str]:
        start, end = date.fromisoformat(ctx["params"]["start"]), date.fromisoformat(ctx["params"]["end"])
        if end >= datetime.now(UTC).date():
            raise ValueError("only closed days can be rebuilt: the stream is still writing today")
        return [str(start + timedelta(days=i)) for i in range((end - start).days + 1)]

    @task(max_active_tis_per_dag=1)
    def rebuild_silver(day: str) -> dict:
        before = trino([f"SELECT count(*) FROM lake.silver.payments WHERE event_date = DATE '{day}'"])[0][0]
        trino([
            f"DELETE FROM lake.silver.payments WHERE event_date = DATE '{day}'",
            sql("rebuild_silver_day.sql", day=day),
        ])
        after = trino([f"SELECT count(*) FROM lake.silver.payments WHERE event_date = DATE '{day}'"])[0][0]
        return {"day": day, "rows_before": before, "rows_after": after}

    rebuilt = rebuild_silver.expand(day=days())

    gold = BashOperator(task_id="dbt_build", bash_command=f"{DBT_CMD} build", env=DBT_ENV)
    rebuilt >> gold


paystream_backfill()
