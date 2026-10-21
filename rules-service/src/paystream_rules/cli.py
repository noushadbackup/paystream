"""paystream-rules: operate the rules service from a shell.

  paystream-rules seed rules/library           publish the reviewed library on first start
  paystream-rules backtest rule.json --duckdb dbt/ci/ci.duckdb --days 1
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser(prog="paystream-rules")
    sub = p.add_subparsers(dest="cmd", required=True)
    seed = sub.add_parser("seed", help="publish library rules that do not exist yet")
    seed.add_argument("directory", type=Path)
    b = sub.add_parser("backtest", help="replay lakehouse history through a rule file")
    b.add_argument("rule", type=Path)
    b.add_argument("--duckdb")
    b.add_argument("--trino")
    b.add_argument("--days", type=float, default=7)
    b.add_argument("--end", help="ISO time; default: now")
    a = p.parse_args()

    if a.cmd == "seed":
        from paystream_rules.publisher import KafkaPublisher, MemoryPublisher
        from paystream_rules.store import RuleStore

        bootstrap = os.environ.get("KAFKA_BOOTSTRAP")
        store = RuleStore(os.environ.get("RULES_DB", "sqlite:///rules.db"),
                          KafkaPublisher(bootstrap) if bootstrap else MemoryPublisher())
        for f in sorted(a.directory.glob("*.json")):
            done = store.bootstrap(json.loads(f.read_text()))
            print(f"{f.name}: {'published v1' if done else 'already present, left as is'}")
        print(f"republished {store.republish()} live versions")
    else:
        from paystream_rules import backtest as bt

        source = bt.DuckDBSource(a.duckdb) if a.duckdb else bt.TrinoSource(a.trino or "localhost")
        end = datetime.fromisoformat(a.end) if a.end else datetime.now(UTC)
        result = bt.run(json.loads(a.rule.read_text()), source, end - timedelta(days=a.days), end)
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
