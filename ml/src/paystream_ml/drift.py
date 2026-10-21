"""Is production traffic still the traffic the champion was validated on?

Population stability index per feature and for the score, against the binned distributions in the
model card. Conventional reading: under 0.1 stable, 0.1 to 0.25 worth a look, above 0.25 the model
is scoring a population it has not seen and should be retrained or reviewed.
"""

from __future__ import annotations

import numpy as np

from paystream_decision.model import Model
from paystream_ml.dataset import Dataset
from paystream_ml.registry import score
from paystream_ml.train import shares

WARN, ALERT = 0.1, 0.25
EPS = 1e-4


def psi(expected: np.ndarray, actual: np.ndarray) -> float:
    e, a = np.clip(np.asarray(expected), EPS, None), np.clip(np.asarray(actual), EPS, None)
    return float(((a - e) * np.log(a / e)).sum())


def status(value: float) -> str:
    return "ALERT" if value > ALERT else "WARN" if value > WARN else "OK"


def report(model: Model, card: dict, recent: Dataset) -> dict:
    ref = card["reference"]
    features = {}
    for n in model.features:
        r = ref["features"][n]
        v = psi(r["shares"], shares(recent.x[:, recent.names.index(n)], r["edges"]))
        features[n] = {"psi": round(v, 4), "status": status(v)}
    s = psi(ref["score"]["shares"], shares(score(model, recent), ref["score"]["edges"]))
    worst = max([s, *(f["psi"] for f in features.values())])
    return {"model": model.version, "n": len(recent), "score_psi": round(s, 4), "status": status(worst),
            "features": dict(sorted(features.items(), key=lambda kv: -kv[1]["psi"]))}
