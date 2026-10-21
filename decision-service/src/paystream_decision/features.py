"""Online features, computed at decision time from the activity buckets Flink keeps in Redis.

Flink writes, per account and device, minute buckets for the last hour, hour buckets for the last
day and recent counterparties (see OnlineFeatures.java for the format). Windows are evaluated here,
at the moment of the decision, so an account that went quiet an hour ago correctly has zero
payments in the last ten minutes even though nothing was written since.

Windows are minute-aligned: "10m" is the current minute plus the nine before it. The payment being
decided is not included: features describe what happened before it.
"""

from __future__ import annotations

from dataclasses import dataclass

import orjson

MINUTE, HOUR = 60_000, 3_600_000

# name -> meaning; the decision log records every one of them for every decision
FEATURES = {
    "s_txn_2m": "payments the sender attempted in the current and previous minute",
    "s_txn_10m": "payments the sender attempted in the last 10 minutes",
    "s_txn_1h": "payments the sender attempted in the last hour",
    "s_failed_10m": "failed attempts by the sender in the last 10 minutes",
    "s_amt_1h_bdt": "money the sender successfully sent in the last hour, taka",
    "s_amt_24h_bdt": "money the sender successfully sent in the last 24 hours, taka",
    "s_cash_out_24h_bdt": "successful cash-out by the sender in the last 24 hours, taka",
    "s_receivers_10m": "distinct accounts the sender paid in the last 10 minutes",
    "s_idle_s": "seconds since the sender's previous payment (null: no history)",
    "r_txn_10m": "payments into the receiver in the last 10 minutes",
    "r_amt_1h_bdt": "money into the receiver in the last hour, taka",
    "r_senders_10m": "distinct accounts that paid the receiver in the last 10 minutes, this sender included",
    "d_accounts_1h": "distinct accounts that paid from this device in the last hour, this sender included",
    "d_txn_10m": "payments from this device in the last 10 minutes",
    "device_known": "the sender has paid from this device before (null: no device history)",
    "amount_bdt": "amount of this payment, taka",
}


@dataclass(frozen=True)
class Activity:
    """One entity's buckets as Flink wrote them."""
    as_of: int
    minutes: list[list[int]]  # [start, n, amount_paisa, failed, cash_out_paisa]
    hours: list[list[int]]
    peers: list[tuple[str, int]]  # (peer, last minute seen)

    @staticmethod
    def parse(raw: bytes | str | None) -> Activity | None:
        if not raw:
            return None
        v = orjson.loads(raw)
        return Activity(v["as_of"], v["m"], v["h"], [(p, t) for p, t in v["p"]])

    def _sum(self, buckets: list[list[int]], size: int, span: int, now: int, col: int) -> int:
        lo = now // size * size - (span - 1) * size
        return sum(b[col] for b in buckets if lo <= b[0] <= now)

    def count(self, minutes: int, now: int) -> int:
        return self._sum(self.minutes, MINUTE, minutes, now, 1)

    def amount(self, minutes: int, now: int) -> int:
        return self._sum(self.minutes, MINUTE, minutes, now, 2)

    def failed(self, minutes: int, now: int) -> int:
        return self._sum(self.minutes, MINUTE, minutes, now, 3)

    def day(self, col: int, now: int) -> int:
        return self._sum(self.hours, HOUR, 24, now, col)

    def peers_within(self, minutes: int, now: int, plus: str | None = None) -> int:
        lo = now // MINUTE * MINUTE - (minutes - 1) * MINUTE
        found = {p for p, t in self.peers if lo <= t <= now}
        if plus is not None:
            found.add(plus)
        return len(found)


def compute(request: dict, sender: Activity | None, receiver: Activity | None, device: Activity | None,
            known_device: bool | None, now: int) -> dict:
    s, r, d = sender, receiver, device
    return {
        "s_txn_2m": s.count(2, now) if s else 0,
        "s_txn_10m": s.count(10, now) if s else 0,
        "s_txn_1h": s.count(60, now) if s else 0,
        "s_failed_10m": s.failed(10, now) if s else 0,
        "s_amt_1h_bdt": s.amount(60, now) / 100 if s else 0.0,
        "s_amt_24h_bdt": s.day(2, now) / 100 if s else 0.0,
        "s_cash_out_24h_bdt": s.day(4, now) / 100 if s else 0.0,
        "s_receivers_10m": s.peers_within(10, now) if s else 0,
        "s_idle_s": max(0, now - s.as_of) / 1000 if s else None,
        "r_txn_10m": r.count(10, now) if r else 0,
        "r_amt_1h_bdt": r.amount(60, now) / 100 if r else 0.0,
        "r_senders_10m": r.peers_within(10, now, request["sender_id"]) if r else 1,
        "d_accounts_1h": d.peers_within(60, now, request["sender_id"]) if d else 1,
        "d_txn_10m": d.count(10, now) if d else 0,
        "device_known": known_device,
        "amount_bdt": request["amount_paisa"] / 100,
    }
