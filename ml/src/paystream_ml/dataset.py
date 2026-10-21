"""Training data: every decision with the features it was made on, joined to what we later learned.

A decision is a positive when its payment carries a FRAUD label and no analyst cleared it
(NOT_FRAUD). Unlabelled decisions are negatives, which is only fair once labels have had time to
arrive: ``maturity`` leaves the most recent decisions out. Features come from the log, never
recomputed, so what the model trains on is exactly what it will be served.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from paystream_decision.features import FEATURES
from paystream_decision.model import vectorize

NAMES = list(FEATURES)

SQL = """
SELECT d.txn_id, d.decided_at, d.features,
       max(CASE WHEN l.disposition = 'FRAUD' THEN 1 ELSE 0 END) AS fraud,
       max(CASE WHEN l.disposition = 'NOT_FRAUD' THEN 1 ELSE 0 END) AS cleared
FROM silver.decisions d
LEFT JOIN silver.fraud_labels l ON l.txn_id = d.txn_id
WHERE d.decided_at >= ? AND d.decided_at < ? AND NOT d.degraded AND d.features IS NOT NULL
GROUP BY d.txn_id, d.decided_at, d.features
ORDER BY d.decided_at
"""


@dataclass
class Dataset:
    x: np.ndarray
    y: np.ndarray
    decided_at: np.ndarray  # epoch millis
    names: list[str]

    def __len__(self) -> int:
        return len(self.y)

    def split(self, at_ms: int) -> tuple[Dataset, Dataset]:
        """Out of time: train before ``at_ms``, validate after. Random splits leak a burst's siblings."""
        a, b = self.decided_at < at_ms, self.decided_at >= at_ms
        return (Dataset(self.x[a], self.y[a], self.decided_at[a], self.names),
                Dataset(self.x[b], self.y[b], self.decided_at[b], self.names))


def from_frame(df: pd.DataFrame, names: list[str] = NAMES) -> Dataset:
    feats = [f if isinstance(f, dict) else json.loads(f) for f in df["features"]]
    x = np.array([vectorize(f, names) for f in feats], dtype=np.float64).reshape(len(feats), len(names))
    y = ((df["fraud"] == 1) & (df["cleared"] == 0)).to_numpy(dtype=np.int8)
    at = pd.to_datetime(df["decided_at"], utc=True).astype("int64").to_numpy() // 1_000_000
    return Dataset(x, y, at, names)


def load(query, start: datetime, end: datetime, maturity: timedelta = timedelta(0)) -> Dataset:
    """``query(sql, params) -> DataFrame``: DuckDB on the fixture or Trino on the lakehouse."""
    return from_frame(query(SQL, (start, end - maturity)))


def duckdb_query(path: str):
    import duckdb

    def q(sql: str, params: tuple) -> pd.DataFrame:
        with duckdb.connect(path, read_only=True) as con:
            return con.execute(sql, list(params)).df()
    return q


def trino_query(host: str, port: int = 8080):
    import trino

    def q(sql: str, params: tuple) -> pd.DataFrame:
        cur = trino.dbapi.connect(host=host, port=port, user="paystream-ml", catalog="lake").cursor()
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])
    return q
