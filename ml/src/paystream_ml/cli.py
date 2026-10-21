"""paystream-ml replay | train | promote | rollback | drift

Reads the lakehouse through Trino (--trino HOST) or a DuckDB file shaped like it (--duckdb PATH),
and writes models to a registry directory the decision service reads (--registry DIR).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np

from paystream_ml import dataset, drift, registry, train

DAY = 86_400_000


def _query(a):
    return dataset.trino_query(a.trino) if a.trino else dataset.duckdb_query(a.duckdb)


def _ts(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


def _bounds(q) -> tuple[int, int]:
    """First and (exclusive) last decision time in the log."""
    df = q("SELECT min(decided_at) AS lo, max(decided_at) AS hi FROM silver.decisions", ())
    ms = [int(datetime.fromisoformat(str(df[c][0])).timestamp() * 1000) for c in ("lo", "hi")]
    return ms[0], ms[1] + 1


def _latest(q) -> int:
    return _bounds(q)[1]


def cmd_replay(a) -> None:
    import duckdb
    import pandas as pd

    from paystream_decision.model import Registry
    from paystream_ml.replay import replay

    models = Registry(Path(a.registry)) if a.registry else None
    with duckdb.connect(a.duckdb) as con:
        pay = con.execute("SELECT txn_id, epoch_ms(event_time) AS event_time, txn_type, sender_id, receiver_id,"
                          " amount_paisa, device_id, district, status, channel FROM silver.payments").df()
        rows = []
        for r in replay((p for p in pay.to_dict("records")), models=models):
            q = r["request"]
            rows.append((r["decision_id"], r["txn_id"], _ts(r["decided_at"]), _ts(r["event_time"]), r["outcome"],
                         ",".join(r["reasons"]), r["risk_score"], r["policy_version"], r["degraded"], r["latency_us"],
                         q["txn_type"], q["sender_id"], q["receiver_id"], q["device_id"], q["amount_paisa"],
                         json.dumps(r["features"]), ",".join(r["flags"]),
                         r.get("model", {}).get("version"), r.get("model", {}).get("score"),
                         r.get("challenger", {}).get("version"), r.get("challenger", {}).get("score")))
        con.execute("""CREATE OR REPLACE TABLE silver.decisions (decision_id varchar, txn_id varchar,
            decided_at timestamptz, event_time timestamptz, outcome varchar, reasons varchar, risk_score double,
            policy_version varchar, degraded boolean, latency_us bigint, txn_type varchar, sender_id varchar,
            receiver_id varchar, device_id varchar, amount_paisa bigint, features varchar, flags varchar,
            model_version varchar, model_score double, challenger_version varchar, challenger_score double)""")
        con.register("frame", pd.DataFrame(rows, columns=[c[0] for c in con.execute("DESCRIBE silver.decisions")
                                                            .fetchall()]))
        con.execute("INSERT INTO silver.decisions SELECT * FROM frame")
    print(f"replayed {len(rows)} decisions into silver.decisions"
          + (f" (scored by {models.champion.version})" if models and models.champion else ""))


def cmd_train(a) -> None:
    q = _query(a)
    first, end = _bounds(q)
    start = max(first, end - int(a.days * DAY))
    cutoff = end - int((end - start) * a.holdout)  # the most recent part is left for promotion
    data = dataset.load(q, _ts(start), _ts(cutoff), timedelta(days=a.maturity_days))
    split = int(np.quantile(data.decided_at, 1 - a.validation))
    spec, card = train.train(data, split, budget=a.budget, params=json.loads(a.params) if a.params else None,
                             window_end_ms=cutoff)
    path = train.save(Path(a.registry), spec, card)
    registry.set_pointer(Path(a.registry), "challenger", spec["version"])
    v = card["validation"]
    print(f"trained {spec['version']} on {card['train']['n']} decisions ({card['train']['positives']} fraud), "
          f"{card['rounds']} trees -> {path}\n"
          f"validation: AP {v['average_precision']:.3f}, ROC AUC {v['roc_auc']:.3f}; holding {a.budget:.1%} "
          f"for review: precision {v['at_threshold']['precision']:.3f}, recall {v['at_threshold']['recall']:.3f}\n"
          f"served model vs LightGBM: max difference {card['max_abs_diff_vs_lightgbm']:.1e}\n"
          f"now the challenger: scored on live traffic and logged, not acted on")


def cmd_promote(a) -> None:
    root = Path(a.registry)
    challenger = registry.pointer(root, "challenger")
    if challenger is None:
        sys.exit("no challenger")
    card = json.loads((root / "models" / challenger / "card.json").read_text())
    q = _query(a)
    holdout = dataset.load(q, _ts(card["window"]["to_ms"]), _ts(_latest(q)), timedelta(days=a.maturity_days))
    if holdout.y.sum() < a.min_positives:  # not an error: try again once more labels have matured
        print(f"waiting: only {int(holdout.y.sum())} labelled frauds since {challenger} was trained; "
              f"need {a.min_positives} to compare fairly")
        return
    print(json.dumps(registry.promote(root, holdout, a.budget), indent=2, default=float))


def cmd_rollback(a) -> None:
    print(json.dumps(registry.rollback(Path(a.registry)), indent=2))


def cmd_drift(a) -> None:
    root = Path(a.registry)
    version = registry.pointer(root, "champion")
    if version is None:
        sys.exit("no champion")
    card = json.loads((root / "models" / version / "card.json").read_text())
    q = _query(a)
    end = _latest(q)
    recent = dataset.load(q, _ts(end - int(a.days * DAY)), _ts(end))
    rep = drift.report(registry.load(root, version), card, recent)
    print(json.dumps(rep, indent=2))
    if rep["status"] == "ALERT" and a.fail_on_alert:
        sys.exit(2)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="paystream-ml")
    src = argparse.ArgumentParser(add_help=False)
    g = src.add_mutually_exclusive_group(required=True)
    g.add_argument("--duckdb")
    g.add_argument("--trino")
    reg = argparse.ArgumentParser(add_help=False)
    reg.add_argument("--registry", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("replay", help="build silver.decisions in a DuckDB file from its silver.payments")
    p.add_argument("--duckdb", required=True)
    p.add_argument("--registry", help="score with this registry's champion and challenger, as live")
    p.set_defaults(fn=cmd_replay)

    p = sub.add_parser("train", parents=[src, reg], help="train a challenger on the decision log and labels")
    p.add_argument("--days", type=float, default=30)
    p.add_argument("--holdout", type=float, default=0.0, help="most recent share of the window kept for promotion")
    p.add_argument("--validation", type=float, default=0.3)
    p.add_argument("--maturity-days", type=float, default=0, help="leave out decisions whose labels may still come")
    p.add_argument("--budget", type=float, default=0.005, help="share of payments the model may hold for review")
    p.add_argument("--params", help="LightGBM parameters as JSON, merged over the defaults")
    p.set_defaults(fn=cmd_train)

    p = sub.add_parser("promote", parents=[src, reg], help="promote the challenger if it beats the champion")
    p.add_argument("--budget", type=float, default=0.005)
    p.add_argument("--maturity-days", type=float, default=0)
    p.add_argument("--min-positives", type=int, default=50)
    p.set_defaults(fn=cmd_promote)

    p = sub.add_parser("rollback", parents=[reg], help="restore the champion the last promotion replaced")
    p.set_defaults(fn=cmd_rollback)

    p = sub.add_parser("drift", parents=[src, reg], help="population stability of recent traffic vs the champion")
    p.add_argument("--days", type=float, default=1)
    p.add_argument("--fail-on-alert", action="store_true")
    p.set_defaults(fn=cmd_drift)

    a = ap.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()
