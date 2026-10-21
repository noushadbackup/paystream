"""paystream-cases consume | dispatch | resync"""

from __future__ import annotations

import argparse
import logging
import os
import signal

from paystream_cases.actions import resync
from paystream_cases.api import effects_from_env
from paystream_cases.store import CaseStore


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="paystream-cases")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("consume", help="turn fraud.alerts and payments.decisions into cases; retry case effects")
    sub.add_parser("dispatch", help="apply pending case effects once and exit")
    sub.add_parser("resync", help="rewrite every freeze and clearance in force from the case database into Redis")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    store = CaseStore(os.environ.get("CASES_DB", "sqlite:///cases.db"))
    effects = effects_from_env()
    if args.cmd == "dispatch":
        print(f"applied {store.dispatch(effects)} effects, {store.outbox_backlog()} pending")
        return
    if args.cmd == "resync":
        if effects.accounts is None:
            raise SystemExit("REDIS_HOST is not set")
        print(f"re-applied {resync(store, effects.accounts)} account actions")
        return

    from paystream_cases.consumer import run

    stopping = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))
    run(store, os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"), effects,
        stop=lambda: bool(stopping),
        resync=(lambda: resync(store, effects.accounts)) if effects.accounts is not None else None)


if __name__ == "__main__":
    main()
