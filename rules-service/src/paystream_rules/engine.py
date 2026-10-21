"""Reference evaluator for analyst rules, used to backtest a rule before it goes live.

Same semantics as Flink's DynamicRuleEvaluator, checked by the shared vectors in rules/conformance:
events are processed in arrival order; for each event that matches a live rule's `when`, the rule
aggregates the matching events with the same group_by value whose event time is in (t - window, t],
and alerts if the threshold is crossed and the (key, rule) cooldown has passed in event time.

Flink drops history once the watermark has moved a full window past it; this engine keeps it. The
results differ only for events that arrive more than a window late, which Flink sends to silver as
late data anyway.
"""

from __future__ import annotations

import bisect
import operator
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

HOUR_MS = 3_600_000


def field_value(event: dict, name: str):
    if name == "hour_dhaka":  # Asia/Dhaka is UTC+6 all year
        return (event["event_time"] // HOUR_MS + 6) % 24
    return event.get(name)


OPS = {
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "in": lambda v, target: v in target,
    "not_in": lambda v, target: v not in target,
    "starts_with": lambda v, target: v.startswith(target),
}


def matches(rule: dict, event: dict) -> bool:
    for c in rule["when"]:
        v = field_value(event, c["field"])
        if v is None:
            if c["op"] not in ("!=", "not_in"):
                return False
        elif not OPS[c["op"]](v, c["value"]):
            return False
    return True


def crosses(rule: dict, observed: float) -> bool:
    th = rule["threshold"]
    return observed >= th["value"] if th["op"] == ">=" else observed > th["value"]


@dataclass(frozen=True)
class Alert:
    rule_id: str
    rule_version: int
    key: str
    trigger_txn_id: str
    event_time: int
    observed: float
    shadow: bool
    action: str


@dataclass
class _History:
    times: list[int] = field(default_factory=list)
    events: list[dict] = field(default_factory=list)

    def add(self, e: dict) -> None:
        i = bisect.bisect_right(self.times, e["event_time"])
        self.times.insert(i, e["event_time"])
        self.events.insert(i, e)

    def between(self, lo_exclusive: int, hi_inclusive: int) -> list[dict]:
        return self.events[bisect.bisect_right(self.times, lo_exclusive):bisect.bisect_right(self.times, hi_inclusive)]


class Engine:
    def __init__(self, rules: Iterable[dict]):
        self.rules = sorted((r for r in rules if r["state"] != "DISABLED"), key=lambda r: r["rule_id"])
        self.history: dict[tuple[str, str], _History] = defaultdict(_History)
        self.last_alert: dict[tuple[str, str], int] = {}
        self.max_key_events = 0

    def run(self, events: Iterable[dict]) -> Iterator[Alert]:
        """`events` in arrival order, each a payment dict with event_time in epoch millis."""
        for e in events:
            yield from self.process(e)

    def process(self, e: dict) -> Iterator[Alert]:
        groupings: list[str] = []
        for r in self.rules:
            if r["group_by"] not in groupings and matches(r, e):
                groupings.append(r["group_by"])
        for g in groupings:
            value = field_value(e, g)
            if value is None:
                continue
            key = (g, str(value))
            h = self.history[key]
            h.add(e)
            self.max_key_events = max(self.max_key_events, len(h.times))
            for r in self.rules:
                if r["group_by"] == g and matches(r, e):
                    observed = self._aggregate(r, h, e["event_time"])
                    if crosses(r, observed) and self._cooled_down(r, key, e["event_time"]):
                        yield Alert(r["rule_id"], r["version"], str(value), e["txn_id"], e["event_time"],
                                    observed, r["state"] == "SHADOW", r.get("action", "REVIEW"))

    def _aggregate(self, r: dict, h: _History, t: int) -> float:
        rows = [x for x in h.between(t - r["window_s"] * 1000, t) if matches(r, x)]
        fn, f = r["aggregate"]["fn"], r["aggregate"].get("field")
        if fn == "COUNT":
            return len(rows)
        if fn == "COUNT_DISTINCT":
            return len({str(v) for x in rows if (v := field_value(x, f)) is not None})
        values = [v for x in rows if (v := field_value(x, f)) is not None]
        if not values:
            return 0
        return {"SUM": sum, "MIN": min, "MAX": max, "AVG": lambda v: sum(v) / len(v)}[fn](values)

    def _cooled_down(self, r: dict, key: tuple[str, str], t: int) -> bool:
        k = (r["rule_id"], *key)
        last = self.last_alert.get(k)
        if last is not None and t >= last and t - last < r.get("cooldown_s", 600) * 1000:
            return False
        self.last_alert[k] = t
        return True
