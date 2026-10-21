"""Champion/challenger promotion over the registry directory the decision service reads.

A new model starts as the challenger: the decision service scores it on live traffic and logs the
score, but never acts on it. Promotion compares challenger and champion on the same, more recent,
labelled decisions, at the same review budget, and swaps the pointer only if the challenger is at
least as good. Every promotion, refusal and rollback is appended to ``history.jsonl``.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from paystream_decision.model import Model
from paystream_ml.dataset import Dataset
from paystream_ml.train import at_threshold, average_precision, budget_threshold

MIN_AP_FIRST_MODEL = 0.2  # the first champion only has to beat a floor
AP_TOLERANCE = 0.01       # a challenger must not be credibly worse on average precision...
RECALL_TOLERANCE = 0.01   # ...nor lose more than a point of recall at the review budget
BOOTSTRAP = 200


def pointer(root: Path, role: str) -> str | None:
    p = Path(root) / role
    return p.read_text().strip() if p.exists() else None


def set_pointer(root: Path, role: str, version: str | None) -> None:
    p = Path(root) / role
    if version is None:
        p.unlink(missing_ok=True)
        return
    if not (Path(root) / "models" / version / "model.json").exists():
        raise FileNotFoundError(f"no model {version} in {root}")
    tmp = p.with_suffix(".tmp")
    tmp.write_text(version + "\n")
    os.replace(tmp, p)  # atomic: the decision service never reads a half-written pointer


def load(root: Path, version: str) -> Model:
    return Model.load(Path(root) / "models" / version / "model.json")


def score(model: Model, data: Dataset) -> np.ndarray:
    idx = [data.names.index(n) for n in model.features]
    raw = np.array([model.raw([row[i] for i in idx]) for row in data.x])
    return 1 / (1 + np.exp(-raw))


def compare(root: Path, versions: list[str], holdout: Dataset, budget: float) -> dict[str, dict]:
    out, scores = {}, {}
    for v in versions:
        s = scores[v] = score(load(root, v), holdout)
        out[v] = {"average_precision": average_precision(holdout.y, s),
                  **at_threshold(holdout.y, s, budget_threshold(s, budget))}
    if len(versions) == 2:
        out["ap_difference_90ci"] = paired_bootstrap(holdout.y, scores[versions[0]], scores[versions[1]])
    return out


def paired_bootstrap(y: np.ndarray, a: np.ndarray, b: np.ndarray, rounds: int = BOOTSTRAP,
                     seed: int = 7) -> list[float]:
    """5th and 95th percentile of AP(a) - AP(b) over resamples of the same decisions: whether a
    difference is larger than the noise of which frauds happened to land in the holdout."""
    rng = np.random.default_rng(seed)
    diffs = []
    for _ in range(rounds):
        i = rng.integers(0, len(y), len(y))
        diffs.append(average_precision(y[i], a[i]) - average_precision(y[i], b[i]))
    return [float(np.percentile(diffs, 5)), float(np.percentile(diffs, 95))]


def promote(root: Path, holdout: Dataset, budget: float = 0.005) -> dict:
    root = Path(root)
    challenger, champion = pointer(root, "challenger"), pointer(root, "champion")
    if challenger is None:
        raise ValueError("no challenger to promote")
    m = compare(root, [v for v in (challenger, champion) if v], holdout, budget)
    c = m[challenger]
    if champion is None:
        ok = c["average_precision"] >= MIN_AP_FIRST_MODEL
        why = f"first champion: AP {c['average_precision']:.3f} vs floor {MIN_AP_FIRST_MODEL}"
    else:
        k = m[champion]
        low, high = m["ap_difference_90ci"]
        ok = low >= -AP_TOLERANCE and c["recall"] >= k["recall"] - RECALL_TOLERANCE
        why = (f"AP {c['average_precision']:.3f} vs {k['average_precision']:.3f} "
               f"(difference 90% CI [{low:+.3f}, {high:+.3f}]), "
               f"recall at {budget:.2%} {c['recall']:.3f} vs {k['recall']:.3f}")
    if ok:
        set_pointer(root, "champion", challenger)
        set_pointer(root, "challenger", None)
    entry = {"at": datetime.now(UTC).isoformat(), "action": "PROMOTED" if ok else "REFUSED",
             "challenger": challenger, "champion_before": champion, "holdout": {"n": len(holdout),
             "positives": int(holdout.y.sum())}, "metrics": m, "why": why}
    _log(root, entry)
    return entry


def rollback(root: Path) -> dict:
    """Put back the champion the last promotion replaced."""
    root = Path(root)
    for entry in reversed(history(root)):
        if entry["action"] == "PROMOTED" and entry["challenger"] == pointer(root, "champion"):
            if entry["champion_before"] is None:
                raise ValueError("the current champion was the first one; nothing to roll back to")
            set_pointer(root, "champion", entry["champion_before"])
            out = {"at": datetime.now(UTC).isoformat(), "action": "ROLLED_BACK",
                   "from": entry["challenger"], "to": entry["champion_before"]}
            _log(root, out)
            return out
    raise ValueError("no promotion of the current champion in the history")


def history(root: Path) -> list[dict]:
    p = Path(root) / "history.jsonl"
    return [json.loads(line) for line in p.read_text().splitlines()] if p.exists() else []


def _log(root: Path, entry: dict) -> None:
    with (Path(root) / "history.jsonl").open("a") as f:
        f.write(json.dumps(entry, default=float) + "\n")
