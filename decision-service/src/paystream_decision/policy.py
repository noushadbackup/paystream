"""The decision policy: what to do with a payment, and why.

Outcomes, strongest first:
  DECLINE    refuse the payment
  REVIEW     hold it for an analyst before money moves
  CHALLENGE  ask the customer to re-authenticate (OTP or PIN again), then proceed
  APPROVE    proceed

Every decision carries reason codes, so a customer-service agent can explain a decline and a
regulator can see why a payment was stopped. Limits are illustrative values for a personal
account; in production they come from the product's limit table.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

STRENGTH = {"APPROVE": 0, "CHALLENGE": 1, "REVIEW": 2, "DECLINE": 3}
OUTGOING = {"P2P", "CASH_OUT", "MERCHANT_PAY", "AIRTIME"}


@dataclass(frozen=True)
class Policy:
    version: str = "2026-10-21.1"
    max_txn_bdt: float = 25_000
    max_cash_out_24h_bdt: float = 50_000
    velocity_2m: int = 6
    failed_10m: int = 3
    new_device_challenge_bdt: float = 10_000
    shared_device_accounts_1h: int = 4
    receiver_fan_in_10m: int = 8
    # When the online store does not answer in time: approve small payments, hold larger ones.
    degraded_review_above_bdt: float = 5_000


@dataclass
class Decision:
    outcome: str = "APPROVE"
    reasons: list[str] = field(default_factory=list)
    risk_score: float = 0.0

    def escalate(self, outcome: str, reason: str, weight: float) -> None:
        if STRENGTH[outcome] > STRENGTH[self.outcome]:
            self.outcome = outcome
        self.reasons.append(reason)
        self.risk_score = 1 - (1 - self.risk_score) * (1 - weight)


def live_flags(flags: dict[str, dict], now_ms: int | None = None) -> dict[str, dict]:
    """Unexpired flags, minus rules an analyst cleared for this entity (an ``ALLOW:<rule>`` entry)."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    live = {rule: f for rule, f in flags.items() if f.get("until", 0) > now_ms}
    allowed = {rule.removeprefix("ALLOW:") for rule, f in live.items() if f.get("action") == "ALLOW"}
    return {rule: f for rule, f in live.items() if f.get("action") != "ALLOW" and rule not in allowed}


def decide(p: Policy, req: dict, f: dict | None, flags: dict[str, dict[str, dict]], now_ms: int) -> Decision:
    """`f` is None when the online store did not answer: only the checks that need no history run."""
    d = Decision()
    amount = req["amount_paisa"] / 100
    personal = req["sender_id"].startswith("U")

    if personal and req["txn_type"] in OUTGOING and amount > p.max_txn_bdt:
        d.escalate("DECLINE", "LIMIT_PER_TXN", 0.0)

    for entity in ("sender", "device"):
        for rule, flag in live_flags(flags.get(entity, {}), now_ms).items():
            if flag.get("action") == "DECLINE":
                d.escalate("DECLINE", f"FLAG_{entity.upper()}:{rule}", 0.9)
            else:
                d.escalate("REVIEW", f"FLAG_{entity.upper()}:{rule}", 0.6)
    if req["txn_type"] == "P2P" and live_flags(flags.get("receiver", {}), now_ms):
        d.escalate("REVIEW", "RECEIVER_FLAGGED", 0.6)

    if f is None:
        if amount > p.degraded_review_above_bdt:
            d.escalate("REVIEW", "DEGRADED_NO_FEATURES", 0.0)
        else:
            d.reasons.append("DEGRADED_NO_FEATURES")
        return d

    if personal and req["txn_type"] == "CASH_OUT" and f["s_cash_out_24h_bdt"] + amount > p.max_cash_out_24h_bdt:
        d.escalate("DECLINE", "LIMIT_CASH_OUT_24H", 0.0)
    if f["s_txn_2m"] + 1 >= p.velocity_2m:
        d.escalate("REVIEW", "VELOCITY", 0.5)
    if f["s_failed_10m"] >= p.failed_10m:
        d.escalate("CHALLENGE", "RECENT_FAILURES", 0.4)
    if f["device_known"] is False and amount >= p.new_device_challenge_bdt:
        d.escalate("CHALLENGE", "NEW_DEVICE_HIGH_VALUE", 0.4)
    if f["d_accounts_1h"] >= p.shared_device_accounts_1h:
        d.escalate("REVIEW", "SHARED_DEVICE", 0.5)
    if req["txn_type"] == "P2P" and f["r_senders_10m"] >= p.receiver_fan_in_10m:
        d.escalate("REVIEW", "RECEIVER_FAN_IN", 0.5)
    return d
