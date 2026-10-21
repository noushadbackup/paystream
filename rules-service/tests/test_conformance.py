"""The vectors in rules/conformance, run against the Python engine. Flink runs the same files."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from paystream_rules.engine import Engine
from paystream_rules.rule import validate

ROOT = Path(__file__).resolve().parents[2] / "rules"
DEFAULTS = {"txn_type": "P2P", "sender_id": "U1", "receiver_id": "U2", "amount_paisa": 100_000, "device_id": "D1",
            "district": "Dhaka", "status": "SUCCESS", "channel": "APP", "currency": "BDT"}


def event(raw: dict) -> dict:
    e = {**DEFAULTS, **{k: v for k, v in raw.items() if k != "at"}}
    e["event_time"] = int(datetime.fromisoformat(raw["at"]).timestamp() * 1000)
    return e


@pytest.mark.parametrize("path", sorted((ROOT / "conformance").glob("*.json")), ids=lambda p: p.name)
def test_vector(path):
    v = json.loads(path.read_text())
    engine = Engine(validate(r) for r in v["rules"])
    actual = sorted((a.rule_id, a.key, a.trigger_txn_id, round(a.observed, 2), a.shadow)
                    for a in engine.run(event(e) for e in v["events"]))
    expected = sorted((x["rule_id"], x["key"], x["trigger_txn_id"], round(float(x["observed"]), 2),
                       x.get("shadow", False)) for x in v["expected"])
    assert actual == expected, v["description"]


@pytest.mark.parametrize("path", sorted((ROOT / "library").glob("*.json")), ids=lambda p: p.name)
def test_library_rule_is_valid(path):
    rule = validate(json.loads(path.read_text()))
    assert path.stem == rule["rule_id"]
