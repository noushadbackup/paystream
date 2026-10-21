from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from paystream_rules.api import create_app, parse_tokens

TOKENS = parse_tokens("a=alice:analyst,b=bob:approver,c=carol:admin")


def h(token):
    return {"Authorization": f"Bearer {token}"}


class FakeSource:
    def payments(self, start, end):
        t = int(datetime(2026, 10, 1, 10, tzinfo=UTC).timestamp() * 1000)
        for i in range(6):
            yield {"txn_id": f"T{i}", "event_time": t + i * 1000, "txn_type": "CASH_OUT", "sender_id": "U1",
                   "receiver_id": "A1", "amount_paisa": 100, "currency": "BDT", "device_id": "D", "district": "Dhaka",
                   "status": "FAILED", "channel": "APP"}

    def labels(self, start, end):
        return [("pin_brute_force", "U1")]


@pytest.fixture
def client(store):
    return TestClient(create_app(store, TOKENS, FakeSource()))


def test_full_review_flow_over_http(client, published, pin_rule):
    assert client.get("/rules").status_code == 401
    r = client.post("/rules", json=pin_rule, headers=h("a"))
    assert r.status_code == 201 and r.json()["version"] == 1

    bt = client.post("/rules/failed-pin-burst/versions/1/backtest", json={"days": 1, "end": "2026-10-02T00:00:00Z"},
                     headers=h("a")).json()
    assert bt["summary"]["alerts"] == 1 and bt["summary"]["precision_vs_labels"] == 1.0
    assert bt["caught_by_scenario"] == {"pin_brute_force": {"incidents": 1, "caught": 1}}

    assert client.post("/rules/failed-pin-burst/versions/1/submit", headers=h("a")).status_code == 200
    assert client.post("/rules/failed-pin-burst/versions/1/approve", json={}, headers=h("a")).status_code == 403
    r = client.post("/rules/failed-pin-burst/versions/1/approve", json={"comment": "ok"}, headers=h("b"))
    assert r.status_code == 200 and published[-1]["version"] == 1

    history = client.get("/rules/failed-pin-burst", headers=h("a")).json()
    assert history[0]["reviewer"] == "bob" and history[0]["backtest"]["summary"]["alerts"] == 1

    assert client.get("/audit", headers=h("b")).status_code == 403
    assert client.get("/audit/verify", headers=h("c")).json()["ok"] is True


def test_validation_errors_are_422_with_reasons(client, pin_rule):
    r = client.post("/rules", json={**pin_rule, "aggregate": {"fn": "SUM"}}, headers=h("a"))
    assert r.status_code == 422
    assert any("needs a field" in p for p in r.json()["detail"]["problems"])


def test_kill_switch_needs_an_approver(client, store, pin_rule):
    store.bootstrap(pin_rule)
    assert client.post("/rules/failed-pin-burst/disable", json={"reason": "x"}, headers=h("a")).status_code == 403
    r = client.post("/rules/failed-pin-burst/disable", json={"reason": "incident 42"}, headers=h("b"))
    assert r.json()["state"] == "DISABLED"
