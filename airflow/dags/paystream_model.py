"""The fraud model's lifecycle on the lakehouse.

Weekly: train a challenger on the last four weeks of decisions whose labels have matured (cases
take days to close). The decision service picks it up within a minute and scores it on live
traffic next to the champion, logging both, acting only on the champion.

Daily: promote the challenger if it is at least as good as the champion on decisions made after it
was trained (it waits until enough of them are labelled), and check that live traffic still looks
like what the champion was validated on. A drift alert fails the task, which is what pages someone.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

ML = os.environ.get("ML_BIN", "paystream-ml")
REGISTRY = os.environ.get("MODEL_REGISTRY", "/models")
TRINO = os.environ.get("TRINO_HOST", "trino")
COMMON = f"--trino {TRINO} --registry {REGISTRY}"
DEFAULTS = {"retries": 1, "retry_delay": timedelta(minutes=10)}


@dag(schedule="0 2 * * 1", start_date=datetime(2026, 10, 1, tzinfo=UTC), catchup=False, default_args=DEFAULTS,
     tags=["paystream", "model"])
def paystream_model_train():
    BashOperator(task_id="train_challenger",
                 bash_command=f"{ML} train {COMMON} --days 35 --maturity-days 7 --budget 0.005")


@dag(schedule="0 4 * * *", start_date=datetime(2026, 10, 1, tzinfo=UTC), catchup=False, default_args=DEFAULTS,
     tags=["paystream", "model"])
def paystream_model_daily():
    promote = BashOperator(task_id="promote_if_better",
                           bash_command=f"{ML} promote {COMMON} --maturity-days 7 --budget 0.005")
    drift = BashOperator(task_id="drift", bash_command=f"{ML} drift {COMMON} --days 1 --fail-on-alert")
    promote >> drift


paystream_model_train()
paystream_model_daily()
