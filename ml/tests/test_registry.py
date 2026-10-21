import numpy as np
import pytest

from conftest import synthetic
from paystream_decision.model import Registry
from paystream_ml import drift, registry, train


def candidate(root, data, **params):
    spec, card = train.train(data, split_at_ms=int(data.decided_at[4000]), budget=0.05, params=params)
    train.save(root, spec, card)
    registry.set_pointer(root, "challenger", spec["version"])
    return spec["version"], card


def test_champion_challenger_lifecycle(data, tmp_path):
    holdout = synthetic(n=4000, seed=2)
    good, _ = candidate(tmp_path, data)
    assert registry.promote(tmp_path, holdout, 0.05)["action"] == "PROMOTED"
    assert registry.pointer(tmp_path, "champion") == good and registry.pointer(tmp_path, "challenger") is None

    noise = synthetic(seed=3)
    noise.y = np.random.default_rng(0).permutation(noise.y)  # learned nothing real
    candidate(tmp_path, noise, seed=11)
    refused = registry.promote(tmp_path, holdout, 0.05)
    assert refused["action"] == "REFUSED" and refused["metrics"]["ap_difference_90ci"][1] < 0
    assert registry.pointer(tmp_path, "champion") == good, "a worse model never goes live"

    other, _ = candidate(tmp_path, data, num_leaves=7, seed=12)
    assert registry.promote(tmp_path, holdout, 0.05)["action"] == "PROMOTED"
    assert Registry(tmp_path).champion.version == other  # what the decision service would now load
    assert registry.rollback(tmp_path)["to"] == good
    assert [e["action"] for e in registry.history(tmp_path)] == ["PROMOTED", "REFUSED", "PROMOTED", "ROLLED_BACK"]


def test_drift_flags_a_shifted_population(data, tmp_path):
    version, card = candidate(tmp_path, data)
    model = registry.load(tmp_path, version)
    same = drift.report(model, card, synthetic(n=3000, seed=5))
    moved = drift.report(model, card, synthetic(n=3000, seed=5, shift=1.5))
    assert same["status"] == "OK"
    assert moved["status"] == "ALERT"
    assert {"s_txn_10m", "amount_bdt"} <= {n for n, f in moved["features"].items() if f["status"] == "ALERT"}


def test_pointers_only_name_models_that_exist(tmp_path):
    with pytest.raises(FileNotFoundError):
        registry.set_pointer(tmp_path, "champion", "nope")
