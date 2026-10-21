import asyncio

import fakeredis
import orjson
import pytest
from fastapi.testclient import TestClient

from conftest import NOW, activity, minute
from paystream_decision.api import create_app
from paystream_decision.log import MemoryDecisionLog


@pytest.fixture
def redis():
    return fakeredis.FakeAsyncRedis()


@pytest.fixture
def log():
    return MemoryDecisionLog()


def body(**kw):
    return {"txn_id": "T1", "txn_type": "P2P", "sender_id": "U1", "receiver_id": "U2", "amount_paisa": 150_000,
            "device_id": "D1", "event_time": NOW, **kw}


def test_decision_reads_flink_state_and_logs_what_it_saw(redis, log):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(redis.set("f:s:U1", activity(minutes=[(minute(0), 5, 0, 0, 0)])))
    loop.run_until_complete(redis.sadd("devs:U1", "D1"))
    loop.run_until_complete(redis.hset("flags:U2", "MULE_FAN_IN",
                                       orjson.dumps({"action": "REVIEW", "until": NOW + 60_000})))
    with TestClient(create_app(redis, log)) as c:
        r = c.post("/v1/decisions", json=body()).json()
    assert r["outcome"] == "REVIEW"
    assert r["reasons"] == ["RECEIVER_FLAGGED", "VELOCITY"]
    assert r["degraded"] is False and r["latency_us"] > 0
    [record] = log
    assert record["features"]["s_txn_2m"] == 5 and record["features"]["device_known"] is True
    assert record["flags"] == ["MULE_FAN_IN"] and record["request"]["txn_id"] == "T1"


def test_an_account_frozen_by_a_case_is_declined(redis, log):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(redis.hset("case:U1", "case-freeze",
                                       orjson.dumps({"action": "DECLINE", "until": NOW + 86_400_000})))
    with TestClient(create_app(redis, log)) as c:
        r = c.post("/v1/decisions", json=body()).json()
    assert r["outcome"] == "DECLINE" and r["reasons"] == ["FLAG_SENDER:case-freeze"]
    assert log[0]["flags"] == ["case-freeze"]


def test_slow_store_degrades_instead_of_blocking_the_payment(log):
    class Slow:
        def pipeline(self, transaction=False):
            return self

        def __getattr__(self, name):
            return lambda *a, **k: None

        async def execute(self):
            await asyncio.sleep(1)

    with TestClient(create_app(Slow(), log, store_timeout_s=0.01)) as c:
        r = c.post("/v1/decisions", json=body(amount_paisa=50_000)).json()
    assert r["outcome"] == "APPROVE" and r["degraded"] is True and r["latency_us"] < 200_000
    assert log[0]["features"] is None


def test_rejects_malformed_requests(redis, log):
    with TestClient(create_app(redis, log)) as c:
        assert c.post("/v1/decisions", json=body(amount_paisa=-5)).status_code == 422
        assert c.post("/v1/decisions", json=body(txn_type="REFUND")).status_code == 422
        assert "decision_latency_seconds" in c.get("/metrics").text


def registry(root, challenger_threshold=0.6):
    import json

    from paystream_decision.model import Registry

    tree = {"f": [0], "t": [5.5], "l": [-1], "r": [-2], "d": [True], "m": [2], "v": [-3.0, 3.0]}
    for version, threshold in (("champ", 0.6), ("chall", challenger_threshold)):
        d = root / "models" / version
        d.mkdir(parents=True)
        (d / "model.json").write_text(json.dumps({"format": "paystream-gbdt/1", "version": version,
                                                  "features": ["s_txn_10m"], "threshold": threshold,
                                                  "trees": [tree]}))
    (root / "champion").write_text("champ")
    (root / "challenger").write_text("chall")
    return Registry(root)


def test_log_record_matches_the_lakehouse_contract(redis, log, tmp_path):
    """schemas/decision_log.example.json is what Flink parses into silver.decisions."""
    from pathlib import Path

    example = orjson.loads((Path(__file__).resolve().parents[2] / "schemas/decision_log.example.json").read_bytes())
    with TestClient(create_app(redis, log, models=registry(tmp_path))) as c:
        c.post("/v1/decisions", json=body())
    record = log[0]
    assert set(record) == set(example)
    assert set(example["request"]) <= set(record["request"])
    assert set(record["model"]) == set(example["model"]) == set(record["challenger"])


def test_the_champion_holds_risky_payments_and_the_challenger_is_only_logged(redis, log, tmp_path):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(redis.set("f:s:U1", activity(minutes=[(minute(0), 9, 0, 0, 0)])))
    with TestClient(create_app(redis, log, models=registry(tmp_path, challenger_threshold=0.0))) as c:
        risky = c.post("/v1/decisions", json=body()).json()
        quiet = c.post("/v1/decisions", json=body(txn_id="T2", sender_id="U5")).json()
        assert c.get("/healthz").json()["model"] == "champ"
    assert risky["outcome"] == "REVIEW" and "MODEL_SCORE" in risky["reasons"]
    assert quiet["outcome"] == "APPROVE", "the challenger's threshold never acts"
    assert log[0]["model"]["version"] == "champ" and log[0]["challenger"]["version"] == "chall"
