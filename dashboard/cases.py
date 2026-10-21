"""The console's view of case management.

On the live stack it talks to the case service over HTTP. In the demo it runs the same CaseStore in
process, seeded with the fixture's alerts, so claiming and closing cases works with no stack at all.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Protocol

import pandas as pd

VERDICTS = ["CONFIRMED_FRAUD", "FALSE_POSITIVE", "INCONCLUSIVE"]


class Cases(Protocol):
    name: str

    def stats(self) -> dict: ...
    def queue(self, limit: int = 50) -> list[dict]: ...
    def claim(self, analyst: str) -> dict | None: ...
    def case(self, case_id: str) -> dict: ...
    def close(self, analyst: str, case_id: str, verdict: str, note: str) -> dict: ...


class HttpCases:
    name = "case service"

    def __init__(self, url: str, token: str):
        import httpx

        self.http = httpx.Client(base_url=url, headers={"Authorization": f"Bearer {token}"}, timeout=5)

    def stats(self) -> dict:
        return self.http.get("/stats").raise_for_status().json()

    def queue(self, limit: int = 50) -> list[dict]:
        return self.http.get("/cases", params={"limit": limit}).raise_for_status().json()

    def claim(self, analyst: str) -> dict | None:
        r = self.http.post("/cases/claim")  # the service knows the analyst from the token
        return None if r.status_code == 404 else r.raise_for_status().json()

    def case(self, case_id: str) -> dict:
        return self.http.get(f"/cases/{case_id}").raise_for_status().json()

    def close(self, analyst: str, case_id: str, verdict: str, note: str) -> dict:
        return self.http.post(f"/cases/{case_id}/close", json={"verdict": verdict, "note": note}
                              ).raise_for_status().json()


class DemoCases:
    """CaseStore on in-memory SQLite, fed the fixture's alerts as the consumer would feed it."""

    name = "in-process demo"

    def __init__(self, backend, now: datetime):
        from paystream_cases.actions import Effects, MemoryLabels
        from paystream_cases.store import CaseStore

        self.store, self.now = CaseStore("sqlite://"), now
        self.labels = MemoryLabels()
        self.effects = Effects(self.labels, None)
        alerts = backend.df("SELECT alert_id, rule, rule_id, account_id, trigger_txn_id, score, action, details,"
                            " shadow, event_time, detected_at FROM silver.fraud_alerts ORDER BY detected_at")
        for a in alerts.to_dict("records"):
            detected = pd.Timestamp(a.pop("detected_at")).to_pydatetime()
            a["event_time"] = pd.Timestamp(a["event_time"]).to_pydatetime()
            a["rule_id"] = None if pd.isna(a["rule_id"]) else a["rule_id"]
            self.store.ingest_alert(a, detected)

    def stats(self) -> dict:
        return self.store.stats(self.now)

    def queue(self, limit: int = 50) -> list[dict]:
        return self.store.queue(limit)

    def claim(self, analyst: str) -> dict | None:
        return self.store.claim_next(analyst)

    def case(self, case_id: str) -> dict:
        return self.store.case(case_id)

    def close(self, analyst: str, case_id: str, verdict: str, note: str) -> dict:
        closure = self.store.close(analyst, case_id, verdict, note)
        self.store.dispatch(self.effects)
        return {"case_id": case_id, "verdict": verdict, "labels": len(closure.labels), "frozen": closure.freeze,
                "cleared": closure.clear_flags, "effects_pending": self.store.outbox_backlog()}


def cases_from_env(backend, now: datetime) -> Cases | None:
    if url := os.environ.get("CASES_URL"):
        return HttpCases(url, os.environ.get("PAYSTREAM_CASES_TOKEN", "dev-analyst"))
    try:
        return DemoCases(backend, now)
    except ImportError:  # paystream_cases not installed
        return None


def queue_frame(rows: list[dict], now: datetime) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    due = pd.to_datetime(df["sla_due_at"], utc=True)
    df["sla_left_min"] = ((due - pd.Timestamp(now)).dt.total_seconds() / 60).round().astype(int)
    df["rules"] = df["rules"].map(lambda r: ", ".join(r))
    df["assignee"] = df["assignee"].fillna("")
    return df[["priority", "subject_id", "status", "assignee", "sla_left_min", "alerts", "max_score", "rules",
               "case_id"]]
