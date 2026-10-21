"""Rule validation: the JSON Schema contract plus the checks a schema cannot express.

Mirrors io.paystream.flink.rules.dynamic.RuleDef. A rule this module accepts, Flink accepts.
"""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path

from jsonschema import Draft202012Validator

REPO = Path(__file__).resolve().parents[3]
SCHEMA_PATH = REPO / "rules" / "schema" / "rule.schema.json"

STRING_FIELDS = {"txn_type", "sender_id", "receiver_id", "device_id", "district", "status", "channel", "currency"}
NUMBER_FIELDS = {"amount_paisa", "hour_dhaka"}


class RuleError(ValueError):
    """A rule that must not be published. `problems` lists every reason, for the analyst to fix in one go."""

    def __init__(self, problems: list[str]):
        super().__init__("; ".join(problems))
        self.problems = problems


@cache
def _validator() -> Draft202012Validator:
    import os

    path = Path(os.environ.get("PAYSTREAM_RULE_SCHEMA", SCHEMA_PATH))
    return Draft202012Validator(json.loads(path.read_text()))


def validate(rule: dict) -> dict:
    """Returns the rule with defaults filled in, or raises RuleError."""
    problems = [f"{'/'.join(map(str, e.absolute_path)) or 'rule'}: {e.message}"
                for e in sorted(_validator().iter_errors(rule), key=lambda e: list(e.absolute_path))]
    if problems:
        raise RuleError(problems)

    for i, c in enumerate(rule.get("when", [])):
        where, field, op, value = f"when/{i}", c["field"], c["op"], c["value"]
        numeric = field in NUMBER_FIELDS
        if op in ("<", "<=", ">", ">=") and not numeric:
            problems.append(f"{where}: {op} needs a numeric field, got {field}")
        elif op == "starts_with" and numeric:
            problems.append(f"{where}: starts_with needs a text field, got {field}")
        elif op in ("in", "not_in"):
            if not isinstance(value, list) or not value:
                problems.append(f"{where}: {op} needs a non-empty list")
            elif not all(_is_number(v) if numeric else isinstance(v, str) for v in value):
                problems.append(f"{where}: {field} compares to {'numbers' if numeric else 'text'}")
        elif not (_is_number(value) if numeric else isinstance(value, str)):
            problems.append(f"{where}: {field} compares to {'numbers' if numeric else 'text'}, got {value!r}")

    agg = rule["aggregate"]
    if agg["fn"] != "COUNT":
        field = agg.get("field")
        if field is None:
            problems.append(f"aggregate: {agg['fn']} needs a field")
        elif agg["fn"] != "COUNT_DISTINCT" and field not in NUMBER_FIELDS:
            problems.append(f"aggregate: {agg['fn']} needs a numeric field, got {field}")
    if problems:
        raise RuleError(problems)
    return {"when": [], "cooldown_s": 600, "action": "REVIEW", **rule}


def _is_number(v) -> bool:
    return isinstance(v, int | float) and not isinstance(v, bool)
