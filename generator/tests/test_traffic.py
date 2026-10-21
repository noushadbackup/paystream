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
    assert set(by_scenario) == {"velocity_burst", "account_takeover", "mule_ring", "agent_float_spike"}
    ato = [e.event for e in events if e.scenario == "account_takeover"]
    assert all(ev["device_id"].startswith("X") and ev["amount_paisa"] >= 18_000 * 100 for ev in ato)


def test_v1_events_have_no_channel(pop):
    assert all("channel" not in e.event for e in run(pop, 5, schema_version=1))
    assert all("channel" in e.event for e in run(pop, 5, schema_version=2))


def test_every_fraud_event_names_its_subject(pop):
    for e in run(pop, 600, fraud_scale=5):
        if e.scenario:
            ev = e.event
            assert e.subject in (ev["sender_id"], ev["receiver_id"])
            if e.scenario == "mule_ring" and ev["txn_type"] == "P2P":
                assert e.subject == ev["receiver_id"]
