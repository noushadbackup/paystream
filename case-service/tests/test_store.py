from datetime import UTC, datetime, timedelta

import pytest

from paystream_cases.store import CaseError, CaseStore

NOW = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)


@pytest.fixture
def store():
    return CaseStore("sqlite://")


def alert(alert_id, account="U1", rule="MULE_FAN_IN", action="REVIEW", score=0.7, txn=None, **kw):
    return {"alert_id": alert_id, "rule": rule, "account_id": account, "trigger_txn_id": txn or f"T-{alert_id}",
            "score": score, "action": action, "details": "", "event_time": int(NOW.timestamp() * 1000), **kw}


def test_alerts_on_one_subject_share_a_case_and_replays_are_ignored(store):
    c1 = store.ingest_alert(alert("a1"), NOW)
    c2 = store.ingest_alert(alert("a2", rule="AGENT_FLOAT_SPIKE", score=0.9), NOW)
    assert c1 == c2
    assert store.ingest_alert(alert("a1"), NOW) is None  # Kafka redelivery
    [q] = store.queue()
    assert q["alerts"] == 2 and q["max_score"] == 0.9 and q["rules"] == ["AGENT_FLOAT_SPIKE", "MULE_FAN_IN"]
    assert store.ingest_alert(alert("a3", account="U2"), NOW) != c1


def test_shadow_alerts_never_become_work(store):
    assert store.ingest_alert(alert("s1", shadow=True), NOW) is None
    assert store.queue() == []


def test_priority_sets_the_sla_and_an_urgent_alert_escalates_an_open_case(store):
    case_id = store.ingest_alert(alert("a1", rule="cash-in-structuring"), NOW)
    assert store.case(case_id)["priority"] == 2
    later = NOW + timedelta(minutes=30)
    store.ingest_alert(alert("a2", rule="failed-pin-burst", action="DECLINE"), later)
    c = store.case(case_id)
    assert c["priority"] == 1
    assert c["sla_due_at"].replace(tzinfo=UTC) == later + timedelta(minutes=15)
    assert [e["kind"] for e in c["history"]] == ["OPENED", "ALERT", "ESCALATED", "ALERT"]


def test_queue_is_ordered_by_priority_then_deadline(store):
    store.ingest_alert(alert("low", account="U3", rule="SOMETHING"), NOW)
    store.ingest_alert(alert("p2", account="U2"), NOW)
    store.ingest_alert(alert("p1", account="U1", rule="NEW_DEVICE_CASH_OUT"), NOW + timedelta(minutes=5))
    assert [c["subject_id"] for c in store.queue()] == ["U1", "U2", "U3"]


def test_held_payments_from_the_decision_service_become_cases(store):
    decision = {"txn_id": "T9", "outcome": "REVIEW", "reasons": ["SHARED_DEVICE"], "risk_score": 0.5,
                "event_time": int(NOW.timestamp() * 1000), "request": {"sender_id": "U7"}}
    case_id = store.ingest_decision(decision, NOW)
    assert store.case(case_id)["alerts"][0]["rule"] == "DECISION_REVIEW"
    assert store.ingest_decision({**decision, "outcome": "APPROVE"}, NOW) is None
    assert store.ingest_decision(decision, NOW) is None


def test_claiming_hands_out_each_case_once(store):
    store.ingest_alert(alert("a1", account="U1"), NOW)
    store.ingest_alert(alert("a2", account="U2"), NOW)
    first, second = store.claim_next("ayesha"), store.claim_next("karim")
    assert {first["subject_id"], second["subject_id"]} == {"U1", "U2"}
    assert first["assignee"] == "ayesha" and first["status"] == "IN_PROGRESS"
    assert store.claim_next("rahim") is None


def test_confirmed_fraud_labels_every_alert_and_freezes(store):
    case_id = store.ingest_alert(alert("a1", txn="T1"), NOW)
    store.ingest_alert(alert("a2", rule="VELOCITY", txn="T2"), NOW)
    store.claim_next("ayesha")
    with pytest.raises(CaseError, match="being worked by ayesha"):
        store.close("karim", case_id, "CONFIRMED_FRAUD")
    closure = store.close("ayesha", case_id, "CONFIRMED_FRAUD", "mule ring, 14 senders")
    assert closure.freeze and not closure.clear_flags
    assert sorted((lb["txn_id"], lb["disposition"], lb["scenario"]) for lb in closure.labels) == [
        ("T1", "FRAUD", "MULE_FAN_IN"), ("T2", "FRAUD", "VELOCITY")]
    assert {lb["case_id"] for lb in closure.labels} == {case_id}
    with pytest.raises(CaseError, match="already closed"):
        store.close("ayesha", case_id, "FALSE_POSITIVE")
    # the next alert on the same subject opens a new case
    assert store.ingest_alert(alert("a3"), NOW) != case_id


def test_a_false_positive_clears_and_suppresses_the_same_rule_for_a_while(store):
    case_id = store.ingest_alert(alert("a1", rule="device-farm"), NOW)
    closure = store.close("ayesha", case_id, "FALSE_POSITIVE", "family shares one phone", now=NOW)
    assert closure.clear_flags and closure.rules == ["device-farm"]
    assert closure.labels[0]["disposition"] == "NOT_FRAUD"

    assert store.ingest_alert(alert("a2", rule="device-farm"), NOW + timedelta(hours=1)) == case_id
    assert store.queue() == [], "the cleared customer is not re-reviewed for the same pattern"
    assert store.stats(NOW)["alerts_suppressed"] == 1
    # a different rule still opens work, and so does the same rule once suppression has lapsed
    assert store.ingest_alert(alert("a3", rule="VELOCITY"), NOW + timedelta(hours=2)) != case_id
    store.close("ayesha", store.queue()[0]["case_id"], "INCONCLUSIVE", now=NOW)
    assert store.ingest_alert(alert("a4", rule="device-farm"), NOW + timedelta(hours=73)) not in (None, case_id)


def test_inconclusive_closes_without_labels_or_actions(store):
    case_id = store.ingest_alert(alert("a1"), NOW)
    closure = store.close("ayesha", case_id, "INCONCLUSIVE")
    assert closure.labels == [] and not closure.freeze and not closure.clear_flags
    with pytest.raises(CaseError, match="verdict must be"):
        store.close("ayesha", store.ingest_alert(alert("a2"), NOW), "MAYBE")


def test_effects_are_dispatched_in_order_and_retried_after_a_failure(store):
    c1 = store.ingest_alert(alert("a1", account="U1"), NOW)
    c2 = store.ingest_alert(alert("a2", account="U2"), NOW)
    store.close("ayesha", c1, "CONFIRMED_FRAUD")
    store.close("ayesha", c2, "FALSE_POSITIVE")
    applied, broken = [], [True]

    def effects(kind, payload):
        if broken[0]:
            raise ConnectionError("kafka down")
        applied.append((kind, payload["subject_id"], payload["verdict"]))

    assert store.dispatch(effects) == 0
    assert store.stats(NOW)["effects_pending"] == 2
    broken[0] = False
    assert store.dispatch(effects) == 2
    assert applied == [("CLOSE", "U1", "CONFIRMED_FRAUD"), ("CLOSE", "U2", "FALSE_POSITIVE")]
    assert store.dispatch(effects) == 0 and store.outbox_backlog() == 0


def test_unfreeze_needs_a_confirmed_case(store):
    with pytest.raises(CaseError, match="no confirmed-fraud case"):
        store.unfreeze("rahim", "U1", "customer verified in branch")
    store.close("ayesha", store.ingest_alert(alert("a1"), NOW), "CONFIRMED_FRAUD")
    store.unfreeze("rahim", "U1", "customer verified in branch")
    assert store.outbox_backlog() == 2


def test_stats(store):
    store.ingest_alert(alert("a1", account="U1", rule="NEW_DEVICE_CASH_OUT"), NOW - timedelta(hours=1))
    store.ingest_alert(alert("a2", account="U2"), NOW)
    s = store.stats(NOW)
    assert s["open"] == 2 and s["open_by_priority"] == {1: 1, 2: 1, 3: 0}
    assert s["sla_breached"] == 1 and s["oldest_open_s"] == 3600


def test_enforcement_lists_what_is_in_force_with_its_original_close_time(store):
    week, day = timedelta(days=30), timedelta(hours=72)
    old = store.ingest_alert(alert("a1", account="U1"), NOW)
    store.close("ayesha", old, "CONFIRMED_FRAUD", now=NOW - timedelta(days=31))
    live = store.ingest_alert(alert("a2", account="U2"), NOW)
    store.close("ayesha", live, "CONFIRMED_FRAUD", now=NOW - timedelta(days=1))
    fp = store.ingest_alert(alert("a3", account="U3"), NOW)
    store.close("ayesha", fp, "FALSE_POSITIVE", now=NOW - timedelta(hours=80))
    [entry] = store.enforcement(week, day, NOW)
    assert entry["subject_id"] == "U2" and entry["freeze"] and entry["labels"] == []
    assert entry["closed_at_ms"] == int((NOW - timedelta(days=1)).timestamp() * 1000)


def test_consumer_routes_each_topic(store):
    import json

    from paystream_cases.consumer import ALERTS, DECISIONS, handle

    assert handle(store, ALERTS, alert("a1")) is not None
    held = {"txn_id": "T5", "outcome": "REVIEW", "reasons": [], "request": {"sender_id": "U9"}, "event_time": 0}
    assert handle(store, DECISIONS, json.dumps(held).encode()) is not None
    assert handle(store, ALERTS, None) is None  # tombstone
    assert {c["subject_id"] for c in store.queue()} == {"U1", "U9"}
