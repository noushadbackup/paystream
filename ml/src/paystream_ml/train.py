"""Fit, measure and export a model, and write it to the registry as a candidate.

Measurement is out of time (train on the earlier part of the window, validate on the later part)
and is reported the way fraud operations use a model: at a review budget. The threshold is the
score above which ``budget`` of all payments would be held for review, chosen on validation traffic
without looking at labels, so the reported precision and recall at that threshold are not tuned to
the labels they are measured on.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from paystream_decision.model import MISSING_NAN, MISSING_NONE, MISSING_ZERO, Model
from paystream_ml.dataset import Dataset

PARAMS = {
    "objective": "binary", "learning_rate": 0.05, "num_leaves": 31, "min_data_in_leaf": 40,
    "feature_fraction": 0.9, "bagging_fraction": 0.8, "bagging_freq": 1, "lambda_l2": 1.0,
    "metric": "average_precision", "verbosity": -1, "seed": 7, "deterministic": True, "num_threads": 1,
}
ROUNDS, EARLY_STOP = 300, 30
MISSING = {"None": MISSING_NONE, "Zero": MISSING_ZERO, "NaN": MISSING_NAN}


# ------------------------------------------------------------------------------------------- metrics

def average_precision(y: np.ndarray, s: np.ndarray) -> float:
    if y.sum() == 0:
        return float("nan")
    order = np.argsort(-s, kind="stable")
    hits = y[order].astype(np.float64)
    precision = np.cumsum(hits) / np.arange(1, len(hits) + 1)
    return float((precision * hits).sum() / hits.sum())


def roc_auc(y: np.ndarray, s: np.ndarray) -> float:
    pos, neg = int(y.sum()), int(len(y) - y.sum())
    if pos == 0 or neg == 0:
        return float("nan")
    ranks = pd.Series(s).rank(method="average").to_numpy()  # ties share their average rank
    return float((ranks[y == 1].sum() - pos * (pos + 1) / 2) / (pos * neg))


def at_threshold(y: np.ndarray, s: np.ndarray, threshold: float) -> dict:
    flagged = s >= threshold
    tp = int((flagged & (y == 1)).sum())
    return {"flagged_share": float(flagged.mean()), "precision": tp / max(1, int(flagged.sum())),
            "recall": tp / max(1, int(y.sum()))}


def budget_threshold(s: np.ndarray, budget: float) -> float:
    return float(np.quantile(s, 1 - budget))


def evaluate(y: np.ndarray, s: np.ndarray, threshold: float) -> dict:
    return {"n": len(y), "positives": int(y.sum()), "average_precision": average_precision(y, s),
            "roc_auc": roc_auc(y, s), "at_threshold": at_threshold(y, s, threshold)}


# -------------------------------------------------------------------------------------------- export

def export(booster: lgb.Booster, names: list[str], version: str, threshold: float) -> dict:
    """LightGBM's dump -> paystream-gbdt/1 (see paystream_decision.model)."""
    trees = [_flatten(info["tree_structure"]) for info in booster.dump_model()["tree_info"]]
    return {"format": "paystream-gbdt/1", "version": version, "features": names, "threshold": threshold,
            "trees": trees}


def _flatten(root: dict) -> dict:
    t = {"f": [], "t": [], "l": [], "r": [], "d": [], "m": [], "v": []}

    def visit(node: dict) -> int:
        if "leaf_value" in node:
            t["v"].append(float(node["leaf_value"]))
            return -len(t["v"])  # leaf k is encoded as -k-1
        if node["decision_type"] != "<=":
            raise ValueError(f"unsupported split {node['decision_type']}")
        i = len(t["f"])
        t["f"].append(int(node["split_feature"]))
        t["t"].append(float(node["threshold"]))
        t["d"].append(bool(node["default_left"]))
        t["m"].append(MISSING[node["missing_type"]])
        t["l"].append(0)
        t["r"].append(0)
        t["l"][i] = visit(node["left_child"])
        t["r"][i] = visit(node["right_child"])
        return i

    visit(root)
    return t


# ----------------------------------------------------------------------------------------- reference

def reference(x: np.ndarray, names: list[str], scores: np.ndarray, bins: int = 10) -> dict:
    """Binned distributions drift is measured against: per feature, and of the score."""
    out = {"features": {}, "score": _histogram(scores, bins)}
    for j, n in enumerate(names):
        out["features"][n] = _histogram(x[:, j], bins)
    return out


def _histogram(v: np.ndarray, bins: int) -> dict:
    present = v[~np.isnan(v)]
    edges = np.unique(np.quantile(present, np.linspace(0, 1, bins + 1)[1:-1])) if len(present) else np.array([])
    return {"edges": edges.tolist(), "shares": shares(v, edges).tolist()}


def shares(v: np.ndarray, edges) -> np.ndarray:
    """Share of values in each bin: [NaN, (-inf, e0], (e0, e1], ..., (e_last, inf)]."""
    edges = np.asarray(edges, dtype=np.float64)
    nan = np.isnan(v)
    counts = np.bincount(np.searchsorted(edges, v[~nan], side="left"), minlength=len(edges) + 1)
    return np.concatenate([[nan.sum()], counts]) / max(1, len(v))


# -------------------------------------------------------------------------------------------- train

def train(data: Dataset, split_at_ms: int, budget: float = 0.005, params: dict | None = None,
          now: datetime | None = None, window_end_ms: int | None = None) -> tuple[dict, dict]:
    """Returns (model spec, model card). ``window_end_ms``: the end of the data the model may have
    seen; promotion measures it only on decisions after that."""
    tr, va = data.split(split_at_ms)
    if tr.y.sum() < 10 or va.y.sum() < 5:
        raise ValueError(f"too few positives to train: {int(tr.y.sum())} train, {int(va.y.sum())} validation")
    p = {**PARAMS, **(params or {})}
    booster = lgb.train(p, lgb.Dataset(tr.x, tr.y, feature_name=data.names, free_raw_data=False),
                        num_boost_round=ROUNDS,
                        valid_sets=[lgb.Dataset(va.x, va.y, feature_name=data.names)],
                        callbacks=[lgb.early_stopping(EARLY_STOP, verbose=False)])
    s_va = booster.predict(va.x, num_iteration=booster.best_iteration)
    threshold = budget_threshold(s_va, budget)

    if booster.best_iteration:
        booster = booster.model_from_string(booster.model_to_string(num_iteration=booster.best_iteration))
    now = now or datetime.now(UTC)
    draft = export(booster, data.names, "pending", threshold)
    digest = hashlib.sha256(json.dumps(draft["trees"], sort_keys=True).encode()).hexdigest()[:8]
    version = f"{now:%Y%m%d%H%M}-{digest}"
    spec = {**draft, "version": version}

    served = Model(spec)  # measure what will be served, not the training library's prediction
    s_served = np.array([served.raw(list(row)) for row in va.x])
    s_served = 1 / (1 + np.exp(-s_served))
    gain = booster.feature_importance("gain")
    card = {
        "version": version, "trained_at": now.isoformat(), "params": p, "rounds": booster.num_trees(),
        "budget": budget, "threshold": threshold,
        "window": {"from_ms": int(data.decided_at.min()), "split_ms": split_at_ms,
                   "to_ms": window_end_ms or int(data.decided_at.max()) + 1},
        "train": {"n": len(tr), "positives": int(tr.y.sum())},
        "validation": evaluate(va.y, s_served, threshold),
        "max_abs_diff_vs_lightgbm": float(np.abs(s_served - s_va).max()),
        "importance_gain": dict(sorted(zip(data.names, map(float, gain), strict=True), key=lambda kv: -kv[1])),
        "reference": reference(va.x, data.names, s_served),
    }
    return spec, card


def save(root: Path, spec: dict, card: dict) -> Path:
    d = Path(root) / "models" / spec["version"]
    d.mkdir(parents=True, exist_ok=True)
    (d / "model.json").write_text(json.dumps(spec, separators=(",", ":")))
    (d / "card.json").write_text(json.dumps(card, indent=2))
    return d
