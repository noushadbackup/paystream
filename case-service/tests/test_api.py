import json
import os
import time

import fakeredis
import pytest
from fastapi.testclient import TestClient

from paystream_cases.actions import AccountActions, Effects, MemoryLabels, resync
from paystream_cases.api import create_app, parse_tokens
from paystream_cases.store import CaseStore

ANALYST = {"Authorization": "Bearer t-a"}
LEAD = {"Authorization": "Bearer t-l"}


@pytest.fixture
def env():
    store = CaseStore("sqlite://")
    # CI points this at a real Redis 7 (the EXPIRE GT/NX semantics matter); locally fakeredis is enough
    url = os.environ.get("PAYSTREAM_TEST_REDIS")
    if url:
        import redis as redis_py

        redis = redis_py.Redis.from_url(url, decode_responses=True)
        redis.flushdb()
    else:
        redis = fakeredis.FakeRedis(decode_responses=True)
    labels = MemoryLabels()
    app = create_app(store, parse_tokens("t-a=ayesha:analyst,t-l=rahim:lead"),
                     Effects(labels, AccountActions(redis)))
    return store, redis, labels, TestClient(app)


def alert(alert_id, account="U1", rule="MULE_FAN_IN", action="REVIEW"):
    return {"alert_id": alert_id, "rule": rule, "account_id": account, "trigger_txn_id": f"T-{alert_id}",
            "score": 0.8, "action": action, "details": "", "event_time": int(time.time() * 1000)}


def test_confirming_fraud_freezes_the_account_for_the_decision_service(env):
    store, redis, labels, c = env
    store.ingest_alert(alert("a1"))
    assert c.get("/cases").status_code == 401
    case = c.post("/cases/claim", headers=ANALYST).json()
    assert case["assignee"] == "ayesha" and case["alerts"][0]["alert_id"] == "a1"
    assert c.post(f"/cases/{case['case_id']}/notes", headers=ANALYST, json={"text": "called the customer"}
                  ).status_code == 201

    r = c.post(f"/cases/{case['case_id']}/close", headers=ANALYST, json={"verdict": "CONFIRMED_FRAUD"}).json()
    assert r == {"case_id": case["case_id"], "verdict": "CONFIRMED_FRAUD", "labels": 1, "frozen": True,
                 "cleared": False, "effects_pending": 0}
    assert labels[0]["disposition"] == "FRAUD" and labels[0]["labelled_by"] == "ayesha"
    freeze = json.loads(redis.hget("case:U1", "case-freeze"))
    assert freeze["action"] == "DECLINE" and freeze["case_id"] == case["case_id"]
    assert redis.ttl("case:U1") > 29 * 24 * 3600

    assert c.post("/accounts/U1/unfreeze", headers=ANALYST, json={"reason": "x"}).status_code == 403
    assert c.post("/accounts/U1/unfreeze", headers=LEAD, json={"reason": "verified in branch"}).status_code == 200
    assert redis.hget("case:U1", "case-freeze") is None


def test_a_false_positive_lifts_the_streaming_flags_and_allows_the_rule(env):
    store, redis, labels, c = env
    redis.hset("flags:U1", mapping={"device-farm": json.dumps({"action": "DECLINE", "until": 9e15}),
                                    "VELOCITY": json.dumps({"action": "REVIEW", "until": 9e15})})
    redis.expire("flags:U1", 3600)
    case_id = store.ingest_alert(alert("a1", rule="device-farm", action="DECLINE"))
    r = c.post(f"/cases/{case_id}/close", headers=ANALYST,
               json={"verdict": "FALSE_POSITIVE", "note": "family phone"}).json()
    assert r["cleared"] and labels[0]["disposition"] == "NOT_FRAUD"
    assert redis.hkeys("flags:U1") == ["VELOCITY"], "only the flags this case covered are lifted"
    allow = json.loads(redis.hget("case:U1", "ALLOW:device-farm"))
    assert allow["action"] == "ALLOW" and allow["until"] - allow["at"] == 72 * 3600 * 1000


def test_close_survives_an_effect_failure_and_reports_it_pending(env):
    store, redis, labels, _ = env

    def broken(_labels):
        raise ConnectionError("broker unavailable")

    app = create_app(store, parse_tokens("t-a=ayesha:analyst"), Effects(broken, AccountActions(redis)))
    case_id = store.ingest_alert(alert("a1"))
    r = TestClient(app).post(f"/cases/{case_id}/close", headers=ANALYST, json={"verdict": "CONFIRMED_FRAUD"})
    assert r.status_code == 200 and r.json()["effects_pending"] == 1
    assert store.case(case_id)["status"] == "CLOSED"
    assert store.dispatch(Effects(labels, AccountActions(redis))) == 1 and len(labels) == 1


def test_errors_map_to_http_codes(env):
    store, _, _, c = env
    assert c.get("/cases/C-nope", headers=ANALYST).status_code == 404
    case_id = store.ingest_alert(alert("a1"))
    assert c.post(f"/cases/{case_id}/close", headers=ANALYST, json={"verdict": "MAYBE"}).status_code == 422
    assert c.post(f"/cases/{case_id}/close", headers=ANALYST, json={"verdict": "INCONCLUSIVE"}).status_code == 200
    assert c.post(f"/cases/{case_id}/close", headers=ANALYST, json={"verdict": "INCONCLUSIVE"}).status_code == 409
    assert c.post("/cases/claim", headers=ANALYST).status_code == 404
    assert c.get("/stats", headers=ANALYST).json()["closed_by_verdict"] == {"INCONCLUSIVE": 1}


def test_redis_losing_everything_is_repaired_from_the_case_database(env):
    store, redis, _, c = env
    frozen = store.ingest_alert(alert("a1", account="U1"))
    cleared = store.ingest_alert(alert("a2", account="U2", rule="device-farm"))
    lifted = store.ingest_alert(alert("a3", account="U3"))
    for case_id, verdict in ((frozen, "CONFIRMED_FRAUD"), (cleared, "FALSE_POSITIVE"), (lifted, "CONFIRMED_FRAUD")):
        c.post(f"/cases/{case_id}/close", headers=ANALYST, json={"verdict": verdict})
    c.post("/accounts/U3/unfreeze", headers=LEAD, json={"reason": "verified"})
    before = {k: redis.hgetall(k) for k in ("case:U1", "case:U2")}

    redis.flushdb()  # restart without persistence, or eviction
    assert resync(store, AccountActions(redis)) == 2
    assert {k: redis.hgetall(k) for k in ("case:U1", "case:U2")} == before, "same entries, same expiry"
    assert redis.exists("case:U3") == 0
