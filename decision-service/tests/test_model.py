import json
import math

import pytest

from paystream_decision.model import Model, Registry

# one tree: s_txn_10m <= 3.5 ? (device_known missing -> right) ... ; NaN handling per mode
TREE = {"f": [0, 1], "t": [3.5, 0.5], "l": [-1, -2], "r": [1, -3], "d": [True, False], "m": [2, 2],
        "v": [-2.0, 0.5, 1.5]}


def spec(version="v1", threshold=0.6, trees=(TREE,)):
    return {"format": "paystream-gbdt/1", "version": version, "features": ["s_txn_10m", "device_known"],
            "threshold": threshold, "trees": list(trees)}


def sigmoid(z):
    return 1 / (1 + math.exp(-z))


def test_tree_walk_and_missing_values():
    m = Model(spec())
    assert m.score({"s_txn_10m": 2, "device_known": True}) == pytest.approx(sigmoid(-2.0))
    assert m.score({"s_txn_10m": 9, "device_known": False}) == pytest.approx(sigmoid(0.5))
    assert m.score({"s_txn_10m": 9, "device_known": True}) == pytest.approx(sigmoid(1.5))
    # NaN mode: a missing count goes the default way (left), a missing device_known goes right
    assert m.score({"s_txn_10m": None, "device_known": True}) == pytest.approx(sigmoid(-2.0))
    assert m.score({"s_txn_10m": 9, "device_known": None}) == pytest.approx(sigmoid(1.5))


def test_missing_modes_none_and_zero():
    none = {**TREE, "m": [0, 0]}  # NaN is read as 0.0
    assert Model(spec(trees=[none])).score({"s_txn_10m": 9, "device_known": None}) == pytest.approx(sigmoid(0.5))
    zero = {**TREE, "m": [1, 1]}  # 0 and NaN are both missing: default direction
    assert Model(spec(trees=[zero])).score({"s_txn_10m": 0, "device_known": True}) == pytest.approx(sigmoid(-2.0))
    leaf_only = {"f": [], "t": [], "l": [], "r": [], "d": [], "m": [], "v": [0.25]}
    assert Model(spec(trees=[TREE, leaf_only])).raw([2, 1]) == pytest.approx(-1.75)


def write(root, s):
    d = root / "models" / s["version"]
    d.mkdir(parents=True)
    (d / "model.json").write_text(json.dumps(s))


def test_registry_follows_the_pointers_and_keeps_serving_through_a_bad_artifact(tmp_path):
    write(tmp_path, spec("v1"))
    (tmp_path / "champion").write_text("v1\n")
    reg = Registry(tmp_path, reload_s=0)
    assert reg.champion.version == "v1" and reg.challenger is None

    write(tmp_path, spec("v2"))
    (tmp_path / "challenger").write_text("v2")
    reg.refresh(force=True)
    assert reg.challenger.version == "v2"

    (tmp_path / "models" / "v3").mkdir()
    (tmp_path / "models" / "v3" / "model.json").write_text("{not json")
    (tmp_path / "champion").write_text("v3")
    assert reg.champion.version == "v1", "a broken model never replaces a working one"
