from conftest import HOUR, NOW, activity, minute
from paystream_decision.features import Activity, compute
from paystream_decision.policy import Policy, decide

P = Policy()


def req(**kw):
    return {"txn_id": "T1", "txn_type": "P2P", "sender_id": "U1", "receiver_id": "U2", "amount_paisa": 100_000,
            "device_id": "D1", **kw}


def feats(r, sender=None, receiver=None, device=None, known=True):
    return compute(r, Activity.parse(sender), Activity.parse(receiver), Activity.parse(device), known, NOW)


def test_windows_are_evaluated_at_decision_time():
    s = activity(minutes=[(minute(-30), 4, 400_000, 0, 0), (minute(-5), 2, 0, 2, 0), (minute(0), 1, 100, 0, 0)],
                 hours=[(NOW // HOUR * HOUR - 3 * HOUR, 9, 2_000_000, 0, 1_500_000)],
                 peers=[("U7", minute(-30)), ("U8", minute(-5)), ("U9", minute(0))])
    f = feats(req(), sender=s)
    assert (f["s_txn_2m"], f["s_txn_10m"], f["s_txn_1h"]) == (1, 3, 7)
    assert f["s_failed_10m"] == 2 and f["s_receivers_10m"] == 2
    assert f["s_amt_24h_bdt"] == 20_000 and f["s_cash_out_24h_bdt"] == 15_000
    assert f["s_idle_s"] == 1.0
    # an hour later nothing new was written, and the short windows are empty
    later = compute(req(), Activity.parse(s), None, None, True, NOW + 2 * HOUR)
    assert later["s_txn_10m"] == 0 and later["s_amt_24h_bdt"] == 20_000


def test_clean_payment_is_approved():
    d = decide(P, req(), feats(req()), {}, NOW)
    assert (d.outcome, d.reasons, d.risk_score) == ("APPROVE", [], 0.0)


def test_limits():
    assert decide(P, req(amount_paisa=2_600_000), feats(req()), {}, NOW).reasons == ["LIMIT_PER_TXN"]
    r = req(txn_type="CASH_OUT", receiver_id="A1", amount_paisa=2_000_000)
    s = activity(hours=[(NOW // HOUR * HOUR, 2, 4_000_000, 0, 4_000_000)])
    d = decide(P, r, feats(r, sender=s), {}, NOW)
    assert d.outcome == "DECLINE" and "LIMIT_CASH_OUT_24H" in d.reasons


def test_a_decline_flag_from_streaming_detection_blocks_the_next_payment():
    flags = {"sender": {"failed-pin-burst": {"action": "DECLINE", "until": NOW + 1000}},
             "receiver": {}, "device": {}}
    d = decide(P, req(), feats(req()), flags, NOW)
    assert d.outcome == "DECLINE" and d.reasons == ["FLAG_SENDER:failed-pin-burst"]
    expired = {"sender": {"failed-pin-burst": {"action": "DECLINE", "until": NOW - 1}}}
    assert decide(P, req(), feats(req()), expired, NOW).outcome == "APPROVE"


def test_analyst_freeze_blocks_and_a_cleared_rule_no_longer_flags():
    frozen = {"sender": {"case-freeze": {"action": "DECLINE", "until": NOW + 1000, "case_id": "C-1"}}}
    assert decide(P, req(), feats(req()), frozen, NOW).reasons == ["FLAG_SENDER:case-freeze"]
    # Flink re-flags the same rule after an analyst cleared it: the clearance wins until it expires
    cleared = {"sender": {"failed-pin-burst": {"action": "DECLINE", "until": NOW + 1000},
                          "ALLOW:failed-pin-burst": {"action": "ALLOW", "until": NOW + 5000}}}
    assert decide(P, req(), feats(req()), cleared, NOW).outcome == "APPROVE"
    assert decide(P, req(), feats(req()), cleared, NOW + 6000).outcome == "APPROVE"  # flag expired too
    lapsed = {"sender": {"failed-pin-burst": {"action": "DECLINE", "until": NOW + 9000},
                         "ALLOW:failed-pin-burst": {"action": "ALLOW", "until": NOW + 5000}}}
    assert decide(P, req(), feats(req()), lapsed, NOW + 6000).outcome == "DECLINE"


def test_new_device_high_value_is_challenged_but_no_history_is_not_new():
    r = req(amount_paisa=1_500_000)
    assert decide(P, r, feats(r, known=False), {}, NOW).reasons == ["NEW_DEVICE_HIGH_VALUE"]
    assert decide(P, r, feats(r, known=None), {}, NOW).outcome == "APPROVE"


def test_device_farm_and_mule_fan_in():
    device = activity(peers=[("U5", minute(-10)), ("U6", minute(-3)), ("U7", minute(0))])
    receiver = activity(peers=[(f"U{i}", minute(-1)) for i in range(10, 17)])
    d = decide(P, req(), feats(req(), receiver=receiver, device=device), {}, NOW)
    assert d.outcome == "REVIEW" and set(d.reasons) == {"SHARED_DEVICE", "RECEIVER_FAN_IN"}
    assert 0.7 < d.risk_score < 0.8


def test_degraded_mode_fails_open_only_for_small_payments():
    small, big = req(amount_paisa=100_000), req(amount_paisa=1_000_000)
    assert decide(P, small, None, {}, NOW).outcome == "APPROVE"
    assert decide(P, big, None, {}, NOW).outcome == "REVIEW"
    assert decide(P, req(amount_paisa=3_000_000), None, {}, NOW).outcome == "DECLINE"  # limits need no history


def test_kafka_settings_come_from_the_environment():
    from paystream_decision.log import kafka_env

    env = {"KAFKA_CFG_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_CFG_SASL_PASSWORD": "x", "KAFKA_CFG_": "", "HOME": "/"}
    assert kafka_env(env) == {"security.protocol": "SASL_SSL", "sasl.password": "x"}
