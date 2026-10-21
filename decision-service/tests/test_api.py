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


def test_log_record_matches_the_lakehouse_contract(redis, log):
    """schemas/decision_log.example.json is what Flink parses into silver.decisions."""
    from pathlib import Path

    example = orjson.loads((Path(__file__).resolve().parents[2] / "schemas/decision_log.example.json").read_bytes())
    with TestClient(create_app(redis, log)) as c:
        c.post("/v1/decisions", json=body())
    record = log[0]
    assert set(record) == set(example)
    assert set(example["request"]) <= set(record["request"])
