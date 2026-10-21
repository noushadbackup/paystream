from paystream_ml.replay import replay

T0 = 1_790_000_040_000  # 40 s into a minute


def pay(i, t, sender="U1", device="D1", status="SUCCESS", **kw):
    return {"txn_id": f"T{i}", "event_time": t, "txn_type": "P2P", "sender_id": sender, "receiver_id": f"U{100 + i}",
            "amount_paisa": 50_000, "device_id": device, "district": "Dhaka", "channel": "APP", "status": status, **kw}


def test_features_see_the_past_only_after_the_pipeline_lag():
    burst = [pay(i, T0 + i * 300) for i in range(6)] + [pay(9, T0 + 5_000)]
    log = list(replay(burst, lag_ms=1_000))
    counts = [r["features"]["s_txn_2m"] for r in log]
    assert counts == [0, 0, 0, 0, 1, 2, 6], "a burst only partly sees itself, as on the live path"
    assert log[0]["features"]["device_known"] is None and log[-1]["features"]["device_known"] is True
    assert log[-1]["features"]["s_receivers_10m"] == 6


def test_one_record_per_payment_in_event_time_order_with_the_live_log_shape():
    events = [pay(2, T0 + 2_000), pay(1, T0), pay(1, T0)]  # out of order, plus an app retry
    log = list(replay(events))
    assert [r["txn_id"] for r in log] == ["T1", "T2"]
    assert {"decision_id", "outcome", "reasons", "features", "request", "flags"} <= set(log[0])
    assert log[0]["decision_id"] == next(replay(events))["decision_id"], "deterministic ids"


def test_failed_attempts_count_but_do_not_teach_known_devices():
    log = list(replay([pay(1, T0, status="FAILED"), pay(2, T0 + 5_000, device="D2")], lag_ms=0))
    assert log[1]["features"]["s_failed_10m"] == 1
    assert log[1]["features"]["device_known"] is None
