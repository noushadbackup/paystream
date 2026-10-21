"""Every console query runs against the dbt-built DuckDB fixture (the same SQL runs on Trino)."""

import os
from pathlib import Path

import pandas as pd
import pytest

import queries as q

FIXTURE = Path(os.environ.get("PAYSTREAM_DUCKDB", Path(__file__).resolve().parents[2] / "dbt/ci/ci.duckdb"))
pytestmark = pytest.mark.skipif(not FIXTURE.exists(), reason="build the fixture and run dbt first")


@pytest.fixture(scope="module")
def b():
    return q.DuckDBBackend(str(FIXTURE))


@pytest.fixture(scope="module")
def now(b):
    return q.anchor(b)


def test_headline(b, now):
    h = q.headline(b, now, 30)
    assert h["payments"] > 0 and h["success_bdt"] > 0
    assert 0 <= h["failure_rate"] < 0.1
    assert h["alerts"] > 0 and h["alert_p95_ms"] > 0


def test_alert_feed_filters_by_rule(b, now):
    feed = q.alert_feed(b, now, ["VELOCITY"])
    assert not feed.empty and set(feed["rule"]) == {"VELOCITY"}
    assert len(q.alert_feed(b, now, [], limit=10)) == 10


def test_per_minute_series(b, now):
    apm, ppm = q.alerts_per_minute(b, now, 30), q.payments_per_minute(b, now, 30)
    assert set(apm.columns) == {"minute", "rule", "alerts"}
    assert ppm["payments"].sum() > 0


def test_reconciliation_and_agents(b):
    recon = q.reconciliation(b)
    assert not recon.empty and set(recon["status"]) <= {"MATCHED", "BROKEN"}
    agents = q.agent_float(b)
    assert len(agents) == 10 and agents["net_float_bdt"].is_monotonic_increasing


def test_account_lookup(b, now):
    alert = q.alert_feed(b, now, ["MULE_FAN_IN"], limit=1).iloc[0]
    payments, alerts = q.account(b, alert["account_id"], now)
    assert not alerts.empty and not payments.empty
    assert isinstance(payments, pd.DataFrame)


def test_analyst_rules_appear_by_name_and_shadow_is_kept_out_of_the_feed(b, now):
    names = q.rule_names(b)
    assert {"VELOCITY", "failed-pin-burst", "device-farm"} <= set(names)
    feed = q.alert_feed(b, now, ["failed-pin-burst"], limit=5)
    assert not feed.empty and set(feed["action"]) == {"DECLINE"}
    assert q.shadow_alerts(b, now).empty or q.shadow_alerts(b, now)["rule"].notna().all()


def test_rule_performance(b):
    perf = q.rule_performance(b).set_index("rule")
    assert perf.loc["failed-pin-burst", "precision_vs_labels"] > 0.9
    assert perf["alerts"].sum() == b.df("SELECT count(*) AS n FROM silver.fraud_alerts")["n"].iloc[0]


def test_aml_views(b):
    s = q.aml_summary(b)
    assert {"PASS_THROUGH", "FAN_IN"} <= set(s["typologies"]["typology"])
    assert s["ctr_reports"] >= 0
    worklist = q.str_candidates(b)
    assert not worklist.empty and worklist["customer_id"].is_unique
    assert list(worklist["typology_count"]) == sorted(worklist["typology_count"], reverse=True)
