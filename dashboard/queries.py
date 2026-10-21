"""Read-only queries behind the Fraud Ops Console.

The same SQL runs on Trino (the live lakehouse) and on DuckDB (the demo fixture), so the console
works with the full stack or with nothing but a file. Every window is anchored on the newest
payment in the data, not the wall clock, which keeps the demo meaningful for a fixture from last
month and is equivalent to "now" on a live stack.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Protocol

import pandas as pd


class Backend(Protocol):
    name: str

    def df(self, sql: str, params: tuple = ()) -> pd.DataFrame: ...


@dataclass
class TrinoBackend:
    host: str = "localhost"
    port: int = 8080
    name: str = "Trino (live lakehouse)"

    def df(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        import trino

        conn = trino.dbapi.connect(host=self.host, port=self.port, user="ops-console", catalog="lake")
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


@dataclass
class DuckDBBackend:
    path: str
    name: str = "DuckDB (demo fixture)"

    def df(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        import duckdb

        with duckdb.connect(self.path, read_only=True) as con:
            return con.execute(sql, list(params)).df()


def backend_from_env() -> Backend:
    if path := os.environ.get("PAYSTREAM_DUCKDB"):
        return DuckDBBackend(path)
    return TrinoBackend(os.environ.get("TRINO_HOST", "localhost"), int(os.environ.get("TRINO_PORT", "8080")))


def anchor(b: Backend) -> pd.Timestamp:
    """The newest event time in the data: 'now' for every window below."""
    return pd.Timestamp(b.df("SELECT max(event_time) AS t FROM silver.payments")["t"].iloc[0])


def headline(b: Backend, now: pd.Timestamp, minutes: int) -> dict:
    p = b.df(
        """
        SELECT count(*) AS payments,
               sum(CASE WHEN status = 'SUCCESS' THEN amount_paisa ELSE 0 END) AS success_paisa,
               count(DISTINCT sender_id) AS active_senders,
               sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed
        FROM silver.payments
        WHERE event_time > ? AND event_time <= ?
        """,
        (now - pd.Timedelta(minutes=minutes), now),
    ).iloc[0]
    a = b.df(
        """
        SELECT ingest_time, detected_at FROM silver.fraud_alerts
        WHERE event_time > ? AND event_time <= ?
        """,
        (now - pd.Timedelta(minutes=minutes), now),
    )
    latency_ms = (pd.to_datetime(a["detected_at"]) - pd.to_datetime(a["ingest_time"])).dt.total_seconds() * 1000
    payments = int(p["payments"] or 0)
    return {
        "payments": payments,
        "success_bdt": int(p["success_paisa"] or 0) / 100,
        "active_senders": int(p["active_senders"] or 0),
        "failure_rate": (int(p["failed"] or 0) / payments) if payments else 0.0,
        "alerts": len(a),
        "alert_p95_ms": float(latency_ms.quantile(0.95)) if len(a) else None,
    }


def alert_feed(b: Backend, now: pd.Timestamp, rules: list[str], limit: int = 50) -> pd.DataFrame:
    rule_filter = " AND rule IN (" + ",".join("?" * len(rules)) + ")" if rules else ""
    return b.df(
        f"""
        SELECT detected_at, rule, account_id, score, details, trigger_txn_id
        FROM silver.fraud_alerts
        WHERE event_time <= ?{rule_filter}
        ORDER BY event_time DESC
        LIMIT {int(limit)}
        """,
        (now, *rules),
    )


def alerts_per_minute(b: Backend, now: pd.Timestamp, minutes: int) -> pd.DataFrame:
    return b.df(
        """
        SELECT date_trunc('minute', event_time) AS minute, rule, count(*) AS alerts
        FROM silver.fraud_alerts
        WHERE event_time > ? AND event_time <= ?
        GROUP BY 1, 2
        ORDER BY 1
        """,
        (now - pd.Timedelta(minutes=minutes), now),
    )


def payments_per_minute(b: Backend, now: pd.Timestamp, minutes: int) -> pd.DataFrame:
    return b.df(
        """
        SELECT date_trunc('minute', event_time) AS minute, txn_type, count(*) AS payments
        FROM silver.payments
        WHERE event_time > ? AND event_time <= ?
        GROUP BY 1, 2
        ORDER BY 1
        """,
        (now - pd.Timedelta(minutes=minutes), now),
    )


def reconciliation(b: Backend) -> pd.DataFrame:
    return b.df(
        """
        SELECT event_date, status, stream_txns, ledger_txns, missing_in_ledger, missing_in_stream,
               amount_mismatches, stream_amount_paisa / 100 AS stream_bdt, ledger_amount_paisa / 100 AS ledger_bdt
        FROM gold.recon_stream_vs_ledger
        ORDER BY event_date DESC
        LIMIT 14
        """
    )


def agent_float(b: Backend, limit: int = 10) -> pd.DataFrame:
    return b.df(
        f"""
        SELECT agent_id, cash_in_paisa / 100 AS cash_in_bdt, cash_out_paisa / 100 AS cash_out_bdt,
               net_float_change_paisa / 100 AS net_float_bdt, transactions
        FROM gold.fct_agent_liquidity_daily
        WHERE event_date = (SELECT max(event_date) FROM gold.fct_agent_liquidity_daily)
        ORDER BY net_float_change_paisa ASC
        LIMIT {int(limit)}
        """
    )


def account(b: Backend, account_id: str, now: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame]:
    payments = b.df(
        """
        SELECT event_time, txn_type, sender_id, receiver_id, amount_paisa / 100 AS amount_bdt, device_id, status
        FROM silver.payments
        WHERE (sender_id = ? OR receiver_id = ?) AND event_time <= ?
        ORDER BY event_time DESC
        LIMIT 100
        """,
        (account_id, account_id, now),
    )
    alerts = b.df(
        "SELECT detected_at, rule, score, details FROM silver.fraud_alerts WHERE account_id = ? ORDER BY detected_at DESC",
        (account_id,),
    )
    return payments, alerts
