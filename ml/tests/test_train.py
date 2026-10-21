import json

import lightgbm as lgb
import numpy as np
import pytest

from paystream_decision.model import Model
from paystream_ml import train


@pytest.mark.parametrize("zero_as_missing", [False, True])
def test_the_served_model_scores_exactly_what_lightgbm_scores(data, zero_as_missing):
    params = {**train.PARAMS, "zero_as_missing": zero_as_missing, "metric": "none"}
    booster = lgb.train(params, lgb.Dataset(data.x, data.y), num_boost_round=40)
    model = Model(json.loads(json.dumps(train.export(booster, data.names, "t", 0.5))))  # through JSON, as served
    served = np.array([model.raw(list(row)) for row in data.x])
    assert np.abs(served - booster.predict(data.x, raw_score=True)).max() < 1e-9
    modes = {m for t in model.trees for m in t.m}
    assert (1 in modes) == zero_as_missing  # the Zero missing mode is really exercised


def test_metrics_against_definitions():
    y = np.array([1, 0, 1, 0, 0, 1])
    s = np.array([0.9, 0.8, 0.7, 0.3, 0.2, 0.1])
    # precision at each fraud in score order: 1/1, 2/3, 3/6
    assert train.average_precision(y, s) == pytest.approx((1 + 2 / 3 + 3 / 6) / 3)
    pairs = [(a, b) for a in s[y == 1] for b in s[y == 0]]
    assert train.roc_auc(y, s) == pytest.approx(sum(a > b for a, b in pairs) / len(pairs))
    assert train.roc_auc(np.array([1, 0]), np.array([0.5, 0.5])) == pytest.approx(0.5)
    t = train.budget_threshold(s, 0.34)
    assert train.at_threshold(y, s, t) == {"flagged_share": pytest.approx(2 / 6), "precision": 0.5,
                                           "recall": pytest.approx(1 / 3)}


def test_train_writes_a_card_with_out_of_time_validation(data, tmp_path):
    spec, card = train.train(data, split_at_ms=int(data.decided_at[4000]), budget=0.05)
    assert card["train"]["n"] == 4000 and card["validation"]["n"] == 2000
    assert card["validation"]["roc_auc"] > 0.75
    assert card["max_abs_diff_vs_lightgbm"] < 1e-9
    assert card["validation"]["at_threshold"]["flagged_share"] == pytest.approx(0.05, abs=0.01)
    assert set(card["reference"]["features"]) == set(data.names)
    d = train.save(tmp_path, spec, card)
    assert Model.load(d / "model.json").version == spec["version"] == card["version"]


def test_refuses_to_train_without_enough_fraud(data):
    data.y[:] = 0
    with pytest.raises(ValueError, match="too few positives"):
        train.train(data, split_at_ms=int(data.decided_at[4000]))
