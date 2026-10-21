"""Replays history from the lakehouse through a rule before anyone approves it.

An analyst sees how many alerts the rule would have raised, on how many accounts, which known
fraud it would have caught, and what it costs to run, on real recent traffic and with the same
engine semantics as Flink.
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from paystream_rules.engine import Engine
from paystream_rules.rule import validate

COLUMNS = ("txn_id", "event_time", "txn_type", "sender_id", "receiver_id", "amount_paisa", "currency",
           "device_id", "district", "status", "channel")
# A key that has to scan more history than this per event makes the rule a hot key in Flink.
MAX_KEY_EVENTS = 50_000


class Source(Protocol):
    def payments(self, start: datetime, end: datetime) -> Iterator[dict]: ...

    def labels(self, start: datetime, end: datetime) -> list[tuple[str, str]]: ...


def _rows(cur, sql: str, params: list) -> Iterator[dict]:
    cur.execute(sql, params)
    while batch := cur.fetchmany(10_000):
        for r in batch:
            e = dict(zip(COLUMNS, r, strict=True))
            e["event_time"] = int(e["event_time"].timestamp() * 1000)
            yield e


PAYMENTS_SQL = f"""SELECT {", ".join(COLUMNS)} FROM silver.payments
WHERE ingest_time >= ? AND ingest_time < ? ORDER BY ingest_time, txn_id"""
LABELS_SQL = """SELECT DISTINCT l.scenario, l.account_id FROM silver.fraud_labels l
JOIN silver.payments p ON p.txn_id = l.txn_id
WHERE p.ingest_time >= ? AND p.ingest_time < ? AND coalesce(l.disposition, 'FRAUD') = 'FRAUD'"""


@dataclass
class DuckDBSource:
    path: str

    def payments(self, start, end):
        import duckdb

        with duckdb.connect(self.path, read_only=True) as con:
            yield from _rows(con.cursor(), PAYMENTS_SQL, [start, end])

    def labels(self, start, end):
        import duckdb

        with duckdb.connect(self.path, read_only=True) as con:
            try:
                return con.execute(LABELS_SQL, [start, end]).fetchall()
            except duckdb.CatalogException:
                return []


@dataclass
class TrinoSource:
    host: str = "trino"
    port: int = 8080

    def _cur(self):
        import trino

        return trino.dbapi.connect(host=self.host, port=self.port, user="rules-backtest", catalog="lake").cursor()

    def payments(self, start, end):
        yield from _rows(self._cur(), PAYMENTS_SQL, [start, end])

    def labels(self, start, end):
        from trino.exceptions import TrinoUserError

        try:
            cur = self._cur()
            cur.execute(LABELS_SQL, [start, end])
            return cur.fetchall()
        except TrinoUserError:  # no labels table yet: precision is reported as unknown
            return []


def run(rule: dict, source: Source, start: datetime, end: datetime, sample: int = 20) -> dict:
    rule = validate({**rule, "version": rule.get("version", 1), "state": "ACTIVE"})
    engine = Engine([rule])
    started, n = time.monotonic(), 0
    alerts = []
    for e in source.payments(start, end):
        n += 1
        alerts.extend(engine.process(e))

    keys = Counter(a.key for a in alerts)
    per_day = Counter(datetime.fromtimestamp(a.event_time / 1000, UTC).date().isoformat() for a in alerts)
    subjects = defaultdict(set)
    for scenario, account in source.labels(start, end):
        subjects[scenario].add(account)
    labelled = set().union(*subjects.values()) if subjects else set()
    caught = {s: {"incidents": len(acc), "caught": len(acc & set(keys))} for s, acc in sorted(subjects.items())}
    summary = {
        "payments_scanned": n,
        "alerts": len(alerts),
        "distinct_keys": len(keys),
        "precision_vs_labels": round(sum(1 for a in alerts if a.key in labelled) / len(alerts), 4)
        if alerts and labelled else None,
        "max_events_per_key": engine.max_key_events,
        "hot_key_risk": engine.max_key_events > MAX_KEY_EVENTS,
        "seconds": round(time.monotonic() - started, 2),
    }
    return {
        "rule_id": rule["rule_id"],
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "summary": summary,
        "alerts_per_day": dict(sorted(per_day.items())),
        "caught_by_scenario": caught,
        "top_keys": keys.most_common(10),
        "sample": [a.__dict__ for a in alerts[:sample]],
    }
