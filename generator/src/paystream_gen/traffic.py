"""Turns a population into a stream of payment events, in the order a gateway would receive them.

Everything is driven by a simulated clock and a seeded RNG, so a run is reproducible:
the same seed produces the same events, which is what makes benchmarks comparable.

Three things make the stream realistic rather than clean:
  * late events: a phone was offline, so the event reaches the gateway minutes after it happened;
  * duplicates: the app retried, so the same txn_id arrives twice;
  * fraud scenarios: labelled bursts of suspicious behaviour, interleaved with normal traffic.
"""

from __future__ import annotations

import heapq
import itertools
import math
import random
from collections.abc import Iterator
from dataclasses import dataclass, field

from paystream_gen.population import OPERATORS, Population

BDT = 100  # paisa per taka
MAX_TXN_PAISA = 25_000 * BDT  # per-transaction limit for personal accounts

# Share of normal traffic by transaction type.
TYPE_MIX = {"P2P": 0.38, "MERCHANT_PAY": 0.22, "CASH_OUT": 0.18, "CASH_IN": 0.14, "AIRTIME": 0.08}
# Median amount in taka and log-normal spread, per type.
AMOUNT_MODEL = {"P2P": (1500, 0.9), "MERCHANT_PAY": (400, 1.0), "CASH_OUT": (2500, 0.8), "CASH_IN": (3000, 0.8)}
AIRTIME_TAKA = [20, 30, 50, 100, 200, 500]
CHANNEL_MIX = {"APP": 0.7, "USSD": 0.2, "AGENT_POS": 0.1}

# Hourly traffic multiplier, Asia/Dhaka local time (UTC+6). Peaks at lunch and after work.
DIURNAL = [0.15, 0.08, 0.05, 0.05, 0.08, 0.2, 0.45, 0.8, 1.1, 1.25, 1.3, 1.35,
           1.45, 1.35, 1.25, 1.2, 1.25, 1.4, 1.6, 1.7, 1.6, 1.3, 0.8, 0.4]


@dataclass
class Config:
    rate_per_s: float = 1000.0
    diurnal: bool = False
    p_late: float = 0.01
    max_late_ms: int = 30 * 60 * 1000
    p_duplicate: float = 0.005
    p_failed: float = 0.02
    p_device_change: float = 0.002  # legit users on a new phone: the fraud rules' false-positive source
    fraud_scale: float = 1.0  # multiplies the per-minute scenario rates below
    schema_version: int = 2
    scenario_rates_per_min: dict[str, float] = field(default_factory=lambda: {
        "velocity_burst": 2.0,
        "account_takeover": 2.0,
        "mule_ring": 0.5,
        "agent_float_spike": 0.2,
        # caught by analyst rules (rules/library), not by the built-in detectors
        "pin_brute_force": 1.0,
        "device_farm": 0.3,
        "structuring": 0.3,
    })


@dataclass(frozen=True)
class Emitted:
    """An event plus when it reaches the gateway, and its fraud label (None for normal traffic)."""
    arrival_ms: int
    event: dict
    scenario: str | None = None
    is_duplicate: bool = False
    subject: str | None = None  # the account (or device) a fraud scenario is about: who should be flagged


class Simulator:
    def __init__(self, population: Population, config: Config, seed: int):
        self.pop = population
        self.cfg = config
        self.rng = random.Random(seed)
        self._seq = itertools.count()
        self._txn = itertools.count()
        self._pending: list[tuple[int, int, Emitted]] = []
        self._seed = seed

    # ---------------------------------------------------------------- public

    def run(self, start_ms: int, duration_s: int) -> Iterator[Emitted]:
        """Yields events in arrival order for [start_ms, start_ms + duration_s)."""
        end_ms = start_ms + duration_s * 1000
        for second in range(duration_s):
            t0 = start_ms + second * 1000
            for _ in range(self._poisson(self._rate_at(t0))):
                self._schedule_normal(t0 + self.rng.randrange(1000))
            for name, per_min in self.cfg.scenario_rates_per_min.items():
                if self.rng.random() < per_min * self.cfg.fraud_scale / 60.0:
                    getattr(self, f"_scenario_{name}")(t0)
            yield from self._drain(until_ms=t0 + 1000)
        # Late events still in flight past the end of the run are delivered too.
        yield from self._drain(until_ms=end_ms + self.cfg.max_late_ms + 10_000)

    # --------------------------------------------------------------- traffic

    def _rate_at(self, t_ms: int) -> float:
        if not self.cfg.diurnal:
            return self.cfg.rate_per_s
        hour_dhaka = (t_ms // 3_600_000 + 6) % 24
        return self.cfg.rate_per_s * DIURNAL[hour_dhaka]

    def _poisson(self, lam: float) -> int:
        if lam > 50:  # normal approximation, fast and close enough at these rates
            return max(0, round(self.rng.gauss(lam, math.sqrt(lam))))
        k, p, threshold = 0, 1.0, math.exp(-lam)
        while True:
            p *= self.rng.random()
            if p <= threshold:
                return k
            k += 1

    def _schedule_normal(self, event_ms: int) -> None:
        rng, pop = self.rng, self.pop
        txn_type = rng.choices(list(TYPE_MIX), list(TYPE_MIX.values()))[0]
        u = pop.pick_user(rng)
        user, district = pop.users[u], pop.user_district[u]
        device = pop.device_of(user)
        if rng.random() < self.cfg.p_device_change:
            device = f"N{rng.getrandbits(40):010x}"

        if txn_type == "P2P":
            sender, receiver = user, pop.contact_of(rng, u)
        elif txn_type == "MERCHANT_PAY":
            sender, receiver = user, pop.pick_merchant(rng)
        elif txn_type == "CASH_OUT":
            sender, receiver = user, pop.pick_agent(rng, district)
        elif txn_type == "CASH_IN":
            agent = pop.pick_agent(rng, district)
            sender, receiver, device = agent, user, f"POS{agent[1:]}"
        else:
            sender, receiver = user, rng.choice(OPERATORS)

        event = self._event(event_ms, txn_type, sender, receiver, self._amount(txn_type), device, district)
        arrival = event_ms
        if rng.random() < self.cfg.p_late:
            arrival += min(int(rng.expovariate(1 / 120_000)) + 1_000, self.cfg.max_late_ms)
        self._push(Emitted(arrival, event))
        if rng.random() < self.cfg.p_duplicate:
            self._push(Emitted(arrival + rng.randrange(200, 5_000), event, is_duplicate=True))

    def _amount(self, txn_type: str) -> int:
        if txn_type == "AIRTIME":
            return self.rng.choice(AIRTIME_TAKA) * BDT
        median, sigma = AMOUNT_MODEL[txn_type]
        taka = self.rng.lognormvariate(math.log(median), sigma)
        return int(min(max(taka, 10), MAX_TXN_PAISA / BDT) * BDT)

    def _event(self, event_ms: int, txn_type: str, sender: str, receiver: str, amount: int,
               device: str, district: str, status: str | None = None) -> dict:
        if status is None:
            status = "FAILED" if self.rng.random() < self.cfg.p_failed else "SUCCESS"
        event = {
            "txn_id": f"T{self._seed:04d}{next(self._txn):012d}",
            "event_time": event_ms,
            "txn_type": txn_type,
            "sender_id": sender,
            "receiver_id": receiver,
            "amount_paisa": amount,
            "currency": "BDT",
            "device_id": device,
            "district": district,
            "status": status,
        }
        if self.cfg.schema_version >= 2:
            event["channel"] = "AGENT_POS" if txn_type == "CASH_IN" else \
                self.rng.choices(list(CHANNEL_MIX), list(CHANNEL_MIX.values()))[0]
        return event

    def _push(self, e: Emitted) -> None:
        heapq.heappush(self._pending, (e.arrival_ms, next(self._seq), e))

    def _drain(self, until_ms: int) -> Iterator[Emitted]:
        while self._pending and self._pending[0][0] < until_ms:
            yield heapq.heappop(self._pending)[2]

    def _fraud(self, scenario: str, subject: str, event: dict) -> None:
        self._push(Emitted(event["event_time"], event, scenario=scenario, subject=subject))

    # ------------------------------------------------------------- scenarios
    # Each scenario schedules future events into the same queue as normal traffic,
    # so they interleave exactly as they would at the gateway.

    def _scenario_velocity_burst(self, t0: int) -> None:
        """A compromised account drains its balance through many quick P2P transfers."""
        rng = self.rng
        u = self.pop.pick_user(rng)
        user, district = self.pop.users[u], self.pop.user_district[u]
        t = t0
        for _ in range(rng.randint(8, 12)):
            t += rng.randint(2_000, 5_000)
            receiver = self.pop.users[rng.randrange(len(self.pop.users))]
            amount = rng.randint(1_000, 5_000) * BDT
            self._fraud("velocity_burst", user,
                        self._event(t, "P2P", user, receiver, amount, self.pop.device_of(user), district, "SUCCESS"))

    def _scenario_account_takeover(self, t0: int) -> None:
        """SIM swap or OTP phishing: a new device logs in and cashes out near the limit."""
        rng = self.rng
        u = self.pop.pick_user(rng)
        user, district = self.pop.users[u], self.pop.user_district[u]
        device = f"X{rng.getrandbits(40):010x}"
        t = t0
        for _ in range(rng.randint(1, 2)):
            t += rng.randint(5_000, 90_000)
            amount = rng.randint(18_000, 25_000) * BDT
            agent = self.pop.pick_agent(rng, district)
            self._fraud("account_takeover", user,
                        self._event(t, "CASH_OUT", user, agent, amount, device, district, "SUCCESS"))

    def _scenario_mule_ring(self, t0: int) -> None:
        """Many victims send money to one mule account within minutes, then the mule cashes out."""
        rng = self.rng
        m = self.pop.pick_user(rng)
        mule, mule_district = self.pop.users[m], self.pop.user_district[m]
        t, total = t0, 0
        for _ in range(rng.randint(12, 20)):
            t += rng.randint(5_000, 25_000)
            v = rng.randrange(len(self.pop.users))
            victim = self.pop.users[v]
            amount = rng.randint(2_000, 9_000) * BDT
            total += amount
            self._fraud("mule_ring", mule, self._event(t, "P2P", victim, mule, amount, self.pop.device_of(victim),
                                                 self.pop.user_district[v], "SUCCESS"))
        agent = self.pop.pick_agent(rng, mule_district)
        self._fraud("mule_ring", mule, self._event(t + 60_000, "CASH_OUT", mule, agent, min(total, MAX_TXN_PAISA),
                                             self.pop.device_of(mule), mule_district, "SUCCESS"))

    def _scenario_agent_float_spike(self, t0: int) -> None:
        """An agent processes an abnormal volume of cash-outs: fake transactions to launder float."""
        rng = self.rng
        district = rng.choice(list(self.pop.agents_by_district))
        agent = self.pop.pick_agent(rng, district)
        t = t0
        for _ in range(rng.randint(30, 50)):
            t += rng.randint(3_000, 15_000)
            u = rng.randrange(len(self.pop.users))
            user = self.pop.users[u]
            amount = rng.randint(5_000, 15_000) * BDT
            self._fraud("agent_float_spike", agent,
                        self._event(t, "CASH_OUT", user, agent, amount, self.pop.device_of(user), district, "SUCCESS"))

    def _scenario_pin_brute_force(self, t0: int) -> None:
        """A stolen phone or SIM: wrong PINs in quick succession, then a cash-out once one works."""
        rng = self.rng
        u = self.pop.pick_user(rng)
        user, district = self.pop.users[u], self.pop.user_district[u]
        device = f"X{rng.getrandbits(40):010x}"
        agent = self.pop.pick_agent(rng, district)
        t = t0
        for _ in range(rng.randint(5, 9)):
            t += rng.randint(3_000, 20_000)
            self._fraud("pin_brute_force", user, self._event(t, "CASH_OUT", user, agent, rng.randint(5_000, 20_000) * BDT,
                                                             device, district, "FAILED"))
        if rng.random() < 0.6:
            self._fraud("pin_brute_force", user, self._event(t + rng.randint(5_000, 30_000), "CASH_OUT", user, agent,
                                                             rng.randint(5_000, 20_000) * BDT, device, district,
                                                             "SUCCESS"))

    def _scenario_device_farm(self, t0: int) -> None:
        """One handset logs into many accounts (bought or rented) and sweeps them into a collector."""
        rng = self.rng
        device = f"F{rng.getrandbits(40):010x}"
        c = self.pop.pick_user(rng)
        collector, district = self.pop.users[c], self.pop.user_district[c]
        t = t0
        for _ in range(rng.randint(4, 8)):
            t += rng.randint(60_000, 400_000)
            account = self.pop.users[rng.randrange(len(self.pop.users))]
            self._fraud("device_farm", device, self._event(t, "P2P", account, collector,
                                                           rng.randint(1_000, 8_000) * BDT, device, district,
                                                           "SUCCESS"))

    def _scenario_structuring(self, t0: int) -> None:
        """Large cash split into several high-value cash-ins at different agents to avoid one big deposit."""
        rng = self.rng
        u = self.pop.pick_user(rng)
        user, district = self.pop.users[u], self.pop.user_district[u]
        t = t0
        for _ in range(rng.randint(3, 5)):
            t += rng.randint(5 * 60_000, 90 * 60_000)
            agent = self.pop.pick_agent(rng, district)
            self._fraud("structuring", user, self._event(t, "CASH_IN", agent, user, rng.randint(20_000, 24_900) * BDT,
                                                         f"POS{agent[1:]}", district, "SUCCESS"))
