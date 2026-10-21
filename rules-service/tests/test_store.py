import pytest
from sqlalchemy import update

from paystream_rules.rule import RuleError
from paystream_rules.store import WorkflowError, audit_log


def test_draft_submit_approve_publishes_once(store, published, pin_rule):
    v1 = store.create_draft("alice", pin_rule)
    assert v1["version"] == 1 and v1["cooldown_s"] == 900
    store.submit("alice", "failed-pin-burst", 1)
    store.approve("bob", "failed-pin-burst", 1, "checked backtest")
    assert [r["version"] for r in published] == [1]
    assert store.rules() == [{"rule_id": "failed-pin-burst",
                              "live": {"version": 1, "state": "ACTIVE", "name": "Failed PIN burst"},
                              "latest": {"version": 1, "status": "PUBLISHED", "author": "alice"}}]


def test_author_cannot_approve_own_rule(store, pin_rule):
    store.create_draft("alice", pin_rule)
    store.submit("alice", "failed-pin-burst", 1)
    with pytest.raises(WorkflowError, match="four-eyes"):
        store.approve("alice", "failed-pin-burst", 1)


def test_only_pending_versions_can_be_approved(store, pin_rule):
    store.create_draft("alice", pin_rule)
    with pytest.raises(WorkflowError, match="is DRAFT, expected PENDING"):
        store.approve("bob", "failed-pin-burst", 1)


def test_new_version_supersedes_and_versions_are_assigned(store, published, pin_rule):
    for threshold in (5, 4):
        v = store.create_draft("alice", {**pin_rule, "threshold": {"op": ">=", "value": threshold}})
        store.submit("alice", v["rule_id"], v["version"])
        store.approve("bob", v["rule_id"], v["version"])
    statuses = [(r["version"], r["status"]) for r in store.history("failed-pin-burst")]
    assert statuses == [(1, "SUPERSEDED"), (2, "PUBLISHED")]
    assert published[-1]["threshold"]["value"] == 4


def test_failed_publish_leaves_nothing_live(tmp_path, pin_rule):
    from paystream_rules.store import RuleStore

    def broken(_):
        raise RuntimeError("broker down")

    s = RuleStore(f"sqlite:///{tmp_path}/r.db", broken)
    s.create_draft("alice", pin_rule)
    s.submit("alice", "failed-pin-burst", 1)
    with pytest.raises(RuntimeError):
        s.approve("bob", "failed-pin-burst", 1)
    assert s.history("failed-pin-burst")[0]["status"] == "PENDING"
    assert [a["action"] for a in s.audit()] == ["CREATE_DRAFT", "SUBMIT"]


def test_kill_switch_publishes_a_disabled_version(store, published, pin_rule):
    store.bootstrap(pin_rule)
    off = store.disable("bob", "failed-pin-burst", "false positives after app release")
    assert off["state"] == "DISABLED" and off["version"] == 2
    assert published[-1] == off
    assert store.audit()[-1]["action"] == "EMERGENCY_DISABLE"


def test_invalid_rule_lists_every_problem(store, pin_rule):
    bad = {**pin_rule, "group_by": "colour", "window_s": 0,
           "when": [{"field": "txn_type", "op": ">", "value": 5}]}
    with pytest.raises(RuleError) as e:
        store.create_draft("alice", bad)
    text = " ".join(e.value.problems)
    assert "group_by" in text and "window_s" in text


def test_audit_chain_detects_tampering(store, pin_rule):
    store.create_draft("alice", pin_rule)
    store.submit("alice", "failed-pin-burst", 1)
    store.approve("bob", "failed-pin-burst", 1)
    assert store.verify_audit()["ok"]
    with store.db.begin() as c:  # someone rewrites who approved it
        c.execute(update(audit_log).where(audit_log.c.seq == 3).values(actor="alice"))
    assert store.verify_audit() == {"ok": False, "rows": 3, "broken_at_seq": 3}


def test_bootstrap_is_idempotent(store, published, pin_rule):
    assert store.bootstrap(pin_rule)["version"] == 1
    assert store.bootstrap(pin_rule) is None
    assert store.republish() == 1
    assert len(published) == 2
