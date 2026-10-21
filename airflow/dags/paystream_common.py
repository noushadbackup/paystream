"""Shared helpers for the PayStream DAGs."""

from __future__ import annotations

import os
from pathlib import Path

SQL_DIR = Path(__file__).parent / "sql"
DBT_DIR = os.environ.get("PAYSTREAM_DBT_DIR", "/opt/paystream/dbt")
DBT = os.environ.get("DBT_BIN", "dbt")
DBT_CMD = f"cd {DBT_DIR} && {DBT} --no-use-colors"
DBT_ENV = {"TRINO_HOST": os.environ.get("TRINO_HOST", "trino"), "TRINO_PORT": "8080",
           "PATH": "/usr/local/bin:/usr/bin:/bin"}

# Tables the streaming jobs append to: small files pile up, so they get compacted daily.
STREAMING_TABLES = [
    "lake.bronze.payment_events",
    "lake.silver.payments",
    "lake.silver.late_payments",
    "lake.silver.fraud_alerts",
    "lake.silver.ledger_transfers",
    "lake.silver.ledger_entries",
    "lake.gold.kpi_minute",
]


def trino(statements: list[str]) -> list[list]:
    """Runs statements in order on Trino and returns the last one's rows."""
    import trino as trino_client

    conn = trino_client.dbapi.connect(host=os.environ.get("TRINO_HOST", "trino"), port=8080, user="airflow",
                                      catalog="lake")
    cur = conn.cursor()
    rows: list[list] = []
    for sql in statements:
        cur.execute(sql)
        rows = cur.fetchall()
    return rows


def sql(name: str, **params: str) -> str:
    return (SQL_DIR / name).read_text().format(**params)
