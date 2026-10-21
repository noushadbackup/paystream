"""The Cases tab's demo backend: the real CaseStore fed the fixture's alerts."""

import os
from pathlib import Path

import pytest

import cases as cs
import queries as q

FIXTURE = Path(os.environ.get("PAYSTREAM_DUCKDB", Path(__file__).resolve().parents[2] / "dbt/ci/ci.duckdb"))
pytest.importorskip("paystream_cases")
pytestmark = pytest.mark.skipif(not FIXTURE.exists(), reason="build the fixture and run dbt first")


def test_demo_cases_group_fixture_alerts_and_close_with_labels():
    b = q.DuckDBBackend(str(FIXTURE))
    now = q.anchor(b)
    cases = cs.DemoCases(b, now)
    s = cases.stats()
    alerts = int(b.df("SELECT count(*) AS n FROM silver.fraud_alerts WHERE NOT shadow")["n"][0])
    assert 0 < s["open"] < alerts, "alerts on the same subject share a case"
    frame = cs.queue_frame(cases.queue(500), now)
    assert list(frame["priority"]) == sorted(frame["priority"])

    case = cases.claim("ayesha")
    r = cases.close("ayesha", case["case_id"], "CONFIRMED_FRAUD", "")
    assert r["frozen"] and r["labels"] == len(case["alerts"]) and r["effects_pending"] == 0
    assert cases.stats()["open"] == s["open"] - 1
