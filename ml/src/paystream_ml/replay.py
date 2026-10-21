"""A decision log from generated traffic, for training when no live stack is running.

On the live stack the training set is ``silver.decisions``: the features the decision service
actually used, logged at decision time. Without a stack, this replays payments through the same
code: an in-memory stand-in for Flink's ``OnlineFeatures`` keeps the same buckets, and the decision
service's own ``features.compute`` and ``policy.decide`` produce each record. Flags from streaming
rules are not replayed.

A payment becomes visible to later decisions ``lag_ms`` after it happened (Flink plus the 200 ms
write coalescing), so a burst's own payments are only partly seen, as on the live path.
"""

from __future__ import annotations

import uuid
from collections import defaultdict, deque
from collections.abc import Iterable, Iterator

from paystream_decision.features import Activity, compute
from paystream_decision.model import Registry, score_decision
from paystream_decision.policy import Policy, decide

MINUTE, HOUR = 60_000, 3_600_000
MINUTES_KEPT, HOURS_KEPT, MAX_PEERS = 60, 24, 500
NAMESPACE = uuid.UUID("5b0e7c1e-2f4a-4d43-9b8e-6a1f0c9d2e31")


class _Entity:
    """One key's buckets, kept and pruned as OnlineFeatures.java does."""

    __slots__ = ("as_of", "hours", "minutes", "peers")

    def __init__(self):
        self.minutes: dict[int, list[int]] = {}
        self.hours: dict[int, list[int]] = {}
        self.peers: dict[str, int] = {}
        self.as_of: int | None = None

    def add(self, p: dict, peer: str) -> None:
        ok = p["status"] == "SUCCESS"
        amt = p["amount_paisa"] if ok else 0
        cash_out = amt if p["txn_type"] == "CASH_OUT" else 0
        for buckets, size in ((self.minutes, MINUTE), (self.hours, HOUR)):
            b = buckets.setdefault(p["event_time"] // size * size, [0, 0, 0, 0])
            b[0] += 1
            b[1] += amt
            b[2] += 0 if ok else 1
            b[3] += cash_out
        minute = p["event_time"] // MINUTE * MINUTE
        if self.peers.get(peer, -1) < minute:
            self.peers[peer] = minute
        if self.as_of is None or p["event_time"] > self.as_of:
            self.as_of = p["event_time"]

    def activity(self) -> Activity:
        now = self.as_of
        lo_m = now // MINUTE * MINUTE - (MINUTES_KEPT - 1) * MINUTE
        lo_h = now // HOUR * HOUR - (HOURS_KEPT - 1) * HOUR
        self.minutes = {k: v for k, v in self.minutes.items() if k >= lo_m}
        self.hours = {k: v for k, v in self.hours.items() if k >= lo_h}
        self.peers = {k: v for k, v in self.peers.items() if v > now - MINUTES_KEPT * MINUTE}
        peers = sorted(self.peers.items(), key=lambda kv: -kv[1])[:MAX_PEERS]
        return Activity(now, [[k, *v] for k, v in sorted(self.minutes.items())],
                        [[k, *v] for k, v in sorted(self.hours.items())], peers)


class OnlineState:
    def __init__(self):
        self.sender: dict[str, _Entity] = defaultdict(_Entity)
        self.receiver: dict[str, _Entity] = defaultdict(_Entity)
        self.device: dict[str, _Entity] = defaultdict(_Entity)
        self.devices: dict[str, set[str]] = defaultdict(set)

    def apply(self, p: dict) -> None:
        self.sender[p["sender_id"]].add(p, p["receiver_id"])
        self.receiver[p["receiver_id"]].add(p, p["sender_id"])
        if p.get("device_id"):
            self.device[p["device_id"]].add(p, p["sender_id"])
            if p["status"] == "SUCCESS" and p["sender_id"].startswith("U"):
                self.devices[p["sender_id"]].add(p["device_id"])

    def _get(self, table: dict[str, _Entity], key: str | None) -> Activity | None:
        e = table.get(key) if key else None
        return e.activity() if e is not None else None

    def features(self, req: dict, now: int) -> dict:
        known = self.devices.get(req["sender_id"])
        known_device = (req.get("device_id") in known) if known else None
        return compute(req, self._get(self.sender, req["sender_id"]), self._get(self.receiver, req["receiver_id"]),
                       self._get(self.device, req.get("device_id")), known_device, now)


REQUEST = ("txn_id", "txn_type", "sender_id", "receiver_id", "amount_paisa", "device_id", "district", "channel",
           "event_time")


def replay(payments: Iterable[dict], policy: Policy | None = None, lag_ms: int = 1_000,
           models: Registry | None = None) -> Iterator[dict]:
    """Decision-log records, one per distinct txn_id, in event-time order. With ``models``, the
    champion and challenger score each payment as the decision service would."""
    policy = policy or Policy()
    state, pending, seen = OnlineState(), deque(), set()
    for p in sorted(payments, key=lambda e: e["event_time"]):
        if p["txn_id"] in seen:
            continue
        seen.add(p["txn_id"])
        now = p["event_time"]
        while pending and pending[0]["event_time"] + lag_ms <= now:
            state.apply(pending.popleft())
        req = {k: p.get(k) for k in REQUEST}
        f = state.features(req, now)
        d = decide(policy, req, f, {}, now)
        scores = score_decision(models, f, d)
        yield {"decision_id": str(uuid.uuid5(NAMESPACE, p["txn_id"])), "txn_id": p["txn_id"], "outcome": d.outcome,
               "reasons": d.reasons, "risk_score": round(d.risk_score, 4), "policy_version": policy.version,
               "degraded": False, "latency_us": 0, "decided_at": now, "event_time": now, "request": req,
               "features": f, "flags": [], **scores}
        pending.append(p)
