import numpy as np
import pytest

from paystream_ml.dataset import NAMES, Dataset


def synthetic(n=6000, seed=1, shift=0.0) -> Dataset:
    """Fraud driven by a few features, with the missing values the real log has (s_idle_s, device_known)."""
    rng = np.random.default_rng(seed)
    x = np.zeros((n, len(NAMES)))
    col = {name: j for j, name in enumerate(NAMES)}
    x[:, col["s_txn_10m"]] = rng.poisson(1 + shift, n)
    x[:, col["amount_bdt"]] = rng.lognormal(6 + shift, 1, n)
    x[:, col["d_accounts_1h"]] = 1 + rng.poisson(0.2, n)
    x[:, col["s_idle_s"]] = np.where(rng.random(n) < 0.2, np.nan, rng.exponential(3000, n))
    x[:, col["device_known"]] = np.where(rng.random(n) < 0.3, np.nan, (rng.random(n) < 0.8).astype(float))
    for name in NAMES:
        if not x[:, col[name]].any():
            x[:, col[name]] = rng.poisson(2, n)
    z = (-5 + 0.9 * x[:, col["s_txn_10m"]] + 0.0004 * x[:, col["amount_bdt"]] + 1.2 * (x[:, col["d_accounts_1h"]] > 1)
         + 1.5 * (x[:, col["device_known"]] == 0))
    y = (rng.random(n) < 1 / (1 + np.exp(-z))).astype(np.int8)
    return Dataset(x, y, np.arange(n, dtype=np.int64) * 1000, list(NAMES))


@pytest.fixture
def data():
    return synthetic()
