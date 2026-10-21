"""Scores a payment with a gradient-boosted tree model, in pure Python.

The training side (``ml/``) fits LightGBM and exports it to this format: flat arrays per tree, so
the model is data that can be reviewed and diffed, and serving needs no native ML library in the
payment's critical path. ``ml`` tests that this evaluator returns what LightGBM returns, to 1e-9.

    {"format": "paystream-gbdt/1", "version": "...", "features": [...], "threshold": 0.42,
     "trees": [{"f": [...], "t": [...], "l": [...], "r": [...], "d": [...], "m": [...], "v": [...]}]}

Per tree: node i splits on feature f[i] at threshold t[i]; l[i] and r[i] are the children, a
negative child -k-1 is leaf k with value v[k]. d[i] says missing values go left; m[i] is the
missing-value mode LightGBM learned (0 none: NaN is treated as 0.0; 1 zero: 0 and NaN are missing;
2 nan: NaN is missing).

A registry directory holds versions and two pointers: ``champion`` (decides) and ``challenger``
(scored on the same traffic and logged, never acted on). Pointers are re-read periodically, so a
promotion goes live without a restart.
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path

MISSING_NONE, MISSING_ZERO, MISSING_NAN = 0, 1, 2


def vectorize(features: dict, names: list[str]) -> list[float]:
    """Feature dict as logged by the decision service -> model input; None becomes NaN."""
    out = []
    for n in names:
        v = features.get(n)
        out.append(math.nan if v is None else float(v))
    return out


@dataclass(frozen=True)
class Tree:
    f: list[int]
    t: list[float]
    l: list[int]
    r: list[int]
    d: list[bool]
    m: list[int]
    v: list[float]

    def value(self, x: list[float]) -> float:
        if not self.f:  # a single-leaf tree
            return self.v[0]
        i = 0
        while i >= 0:
            xv = x[self.f[i]]
            mode = self.m[i]
            if math.isnan(xv):
                if mode == MISSING_NONE:
                    xv = 0.0
                else:
                    i = self.l[i] if self.d[i] else self.r[i]
                    continue
            elif mode == MISSING_ZERO and xv == 0.0:
                i = self.l[i] if self.d[i] else self.r[i]
                continue
            i = self.l[i] if xv <= self.t[i] else self.r[i]
        return self.v[-i - 1]


class Model:
    def __init__(self, spec: dict):
        if spec.get("format") != "paystream-gbdt/1":
            raise ValueError(f"unsupported model format {spec.get('format')}")
        self.version: str = spec["version"]
        self.features: list[str] = spec["features"]
        self.threshold: float = spec["threshold"]
        self.trees = [Tree(**t) for t in spec["trees"]]

    @staticmethod
    def load(path: Path) -> Model:
        return Model(json.loads(Path(path).read_text()))

    def raw(self, x: list[float]) -> float:
        return sum(t.value(x) for t in self.trees)

    def score(self, features: dict) -> float:
        """Probability of fraud."""
        return 1.0 / (1.0 + math.exp(-self.raw(vectorize(features, self.features))))


def score_decision(models: Registry | None, f: dict | None, decision) -> dict:
    """The champion scores a payment that has features and holds it for review above its threshold;
    the challenger is scored on the same features and only logged. Returns the decision-log fields."""
    if f is None or models is None or (champion := models.champion) is None:
        return {}
    p = champion.score(f)
    out = {"model": {"version": champion.version, "score": round(p, 6)}}
    if p >= champion.threshold:
        decision.escalate("REVIEW", "MODEL_SCORE", p)
    if (challenger := models.challenger) is not None:
        out["challenger"] = {"version": challenger.version, "score": round(challenger.score(f), 6)}
    return out


class Registry:
    """``root/models/<version>/model.json`` plus ``root/champion`` and ``root/challenger`` pointer files."""

    def __init__(self, root: Path, reload_s: float = 30.0):
        self.root, self.reload_s = Path(root), reload_s
        self._checked = 0.0
        self._pointers: dict[str, str | None] = {}
        self._models: dict[str, Model | None] = {"champion": None, "challenger": None}
        self.refresh(force=True)

    def refresh(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._checked < self.reload_s:
            return
        self._checked = now
        for role in ("champion", "challenger"):
            p = self.root / role
            version = p.read_text().strip() if p.exists() else None
            if version != self._pointers.get(role):
                # a bad artifact keeps the previous model serving rather than failing payments
                try:
                    self._models[role] = Model.load(self.root / "models" / version / "model.json") if version else None
                    self._pointers[role] = version
                except (OSError, ValueError, KeyError):
                    pass

    @property
    def champion(self) -> Model | None:
        self.refresh()
        return self._models["champion"]

    @property
    def challenger(self) -> Model | None:
        return self._models["challenger"]
