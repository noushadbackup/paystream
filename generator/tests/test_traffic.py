import json
from collections import Counter

import pytest

from paystream_gen.population import Population
from paystream_gen.traffic import MAX_TXN_PAISA, Config, Simulator

START = 1_790_000_000_000


@pytest.fixture(scope="module")
def pop():
    return Population.build(n_users=5_000, n_merchants=200, n_agents=100, seed=7)


def run(pop, seconds=120, **kw):
    return list(Simulator(pop, Config(rate_per_s=200, **kw), seed=1).run(START, seconds))


def test_same_seed_same_stream(pop):
    a = [e.event for e in run(pop, 20)]
    b = [e.event for e in run(pop, 20)]
    assert a == b


def test_rate_is_close_to_target(pop):
    events = [e for e in run(pop, 120, fraud_scale=0) if not e.is_duplicate]
    assert 200 * 120 * 0.95 < len(events) < 200 * 120 * 1.05


def test_arrival_order_and_late_events(pop):
    events = run(pop, 120, p_late=0.05)
    arrivals = [e.arrival_ms for e in events]
    assert arrivals == sorted(arrivals)
    late = [e for e in events if e.arrival_ms - e.event["event_time"] > 1000]
    assert 0.03 < len(late) / len(events) < 0.07
    assert max(e.arrival_ms - e.event["event_time"] for e in events) <= 30 * 60 * 1000


def test_duplicates_reuse_txn_id(pop):
    events = run(pop, 120, p_duplicate=0.02)
    dups = [e for e in events if e.is_duplicate]
    assert dups
    by_id = Counter(e.event["txn_id"] for e in events)
    assert all(by_id[d.event["txn_id"]] == 2 for d in dups)


def test_amounts_are_positive_integers_within_limit(pop):
    for e in run(pop, 30):
        amt = e.event["amount_paisa"]
        assert isinstance(amt, int) and 0 < amt <= MAX_TXN_PAISA


def test_cash_in_comes_from_agent_cash_out_goes_to_agent(pop):
    for e in run(pop, 30, fraud_scale=0):
        ev = e.event
        if ev["txn_type"] == "CASH_IN":
            assert ev["sender_id"].startswith("A") and ev["receiver_id"].startswith("U")
        if ev["txn_type"] == "CASH_OUT":
            assert ev["receiver_id"].startswith("A")


def test_fraud_scenarios_are_labelled_and_shaped(pop):
    events = run(pop, 600, fraud_scale=5)
    by_scenario = Counter(e.scenario for e in events if e.scenario)
    assert set(by_scenario) == {"velocity_burst", "account_takeover", "mule_ring", "agent_float_spike",
                                "pin_brute_force", "device_farm", "structuring"}
    ato = [e.event for e in events if e.scenario == "account_takeover"]
    assert all(ev["device_id"].startswith("X") and ev["amount_paisa"] >= 18_000 * 100 for ev in ato)


def test_v1_events_have_no_channel(pop):
    assert all("channel" not in e.event for e in run(pop, 5, schema_version=1))
    assert all("channel" in e.event for e in run(pop, 5, schema_version=2))


def test_every_fraud_event_names_its_subject(pop):
    for e in run(pop, 600, fraud_scale=5):
        if e.scenario:
            ev = e.event
            assert e.subject in (ev["sender_id"], ev["receiver_id"], ev["device_id"])
            if e.scenario == "mule_ring" and ev["txn_type"] == "P2P":
                assert e.subject == ev["receiver_id"]


def test_new_scenarios_have_the_shape_their_rules_look_for(pop):
    events = run(pop, 3600, fraud_scale=3)
    pins = [e.event for e in events if e.scenario == "pin_brute_force"]
    assert sum(ev["status"] == "FAILED" for ev in pins) >= 5
    farm = Counter(e.subject for e in events if e.scenario == "device_farm")
    assert all(n >= 4 for n in farm.values())
    cash_ins = [e.event for e in events if e.scenario == "structuring"]
    assert all(ev["txn_type"] == "CASH_IN" and ev["amount_paisa"] >= 20_000 * 100 for ev in cash_ins)


def test_jsonl_labels_match_events(tmp_path):
    from paystream_gen.cli import main

    events, labels = tmp_path / "events.jsonl", tmp_path / "labels.jsonl"
    main(["--rate", "200", "--duration", "600", "--users", "5000", "--merchants", "500", "--agents", "200",
          "--fraud-scale", "5", "--start", "2026-10-01T00:00:00+00:00", "--out", str(events),
          "--labels-out", str(labels)])
    by_id = {e["txn_id"]: e for e in map(json.loads, events.read_text().splitlines())}
    rows = [json.loads(x) for x in labels.read_text().splitlines()]
    assert rows and {r["scenario"] for r in rows} >= {"velocity_burst", "mule_ring"}
    assert all(r["txn_id"] in by_id and r["account_id"] for r in rows)
    assert len({r["txn_id"] for r in rows}) == len(rows)  # duplicates are not labelled twice


def test_no_personal_account_dominates():
    from paystream_gen.population import USER_WEIGHT_CAP

    p = Population.build(n_users=200_000, n_merchants=100, n_agents=50, seed=3)
    w = [b - a for a, b in zip([0.0, *p.user_cum_weights], p.user_cum_weights, strict=False)]
    mean = sum(w) / len(w)
    assert max(w) <= USER_WEIGHT_CAP * (1 + 1e-9)
    assert max(w) / mean < 25  # uncapped, the tail reaches hundreds of times the mean
