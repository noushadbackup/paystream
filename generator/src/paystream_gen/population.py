"""Who transacts: users, merchants and agents, with skewed activity.

Real MFS traffic is heavily skewed: a small share of accounts produces most
transactions. Activity weights follow a Pareto distribution so hot keys show
up in Kafka partitions and Flink state the way they would in production.
"""

from __future__ import annotations

import bisect
import itertools
import random
from dataclasses import dataclass

# A subset of Bangladesh's 64 districts, weighted roughly by population.
DISTRICTS: dict[str, float] = {
    "Dhaka": 14.0, "Chattogram": 8.0, "Cumilla": 5.5, "Mymensingh": 5.0, "Gazipur": 5.0,
    "Narayanganj": 3.5, "Sylhet": 3.5, "Rajshahi": 3.0, "Khulna": 2.5, "Bogura": 3.5,
    "Rangpur": 3.0, "Barishal": 2.5, "Noakhali": 3.0, "Jashore": 3.0, "Tangail": 3.5,
    "Faridpur": 2.0, "Cox's Bazar": 2.5, "Dinajpur": 3.0, "Pabna": 2.5, "Kushtia": 2.0,
}

OPERATORS = ["OP_GP", "OP_ROBI", "OP_BL", "OP_TT"]


@dataclass(frozen=True)
class Population:
    users: list[str]
    user_district: list[str]
    user_cum_weights: list[float]
    merchants: list[str]
    merchant_cum_weights: list[float]
    agents_by_district: dict[str, list[str]]

    @classmethod
    def build(cls, n_users: int, n_merchants: int, n_agents: int, seed: int) -> Population:
        rng = random.Random(seed)
        names = list(DISTRICTS)
        dweights = list(DISTRICTS.values())

        users = [f"U{i:07d}" for i in range(n_users)]
        user_district = rng.choices(names, dweights, k=n_users)
        user_cum = list(itertools.accumulate(rng.paretovariate(1.6) for _ in range(n_users)))

        merchants = [f"M{i:05d}" for i in range(n_merchants)]
        merchant_cum = list(itertools.accumulate(rng.paretovariate(1.2) for _ in range(n_merchants)))

        agents_by_district: dict[str, list[str]] = {d: [] for d in names}
        for i, d in enumerate(rng.choices(names, dweights, k=n_agents)):
            agents_by_district[d].append(f"A{i:05d}")
        # Every district needs at least one agent for cash-in/out to work.
        for j, d in enumerate(names):
            if not agents_by_district[d]:
                agents_by_district[d].append(f"A{n_agents + j:05d}")

        return cls(users, user_district, user_cum, merchants, merchant_cum, agents_by_district)

    def pick_user(self, rng: random.Random) -> int:
        """Index of an active user, weighted by activity."""
        x = rng.random() * self.user_cum_weights[-1]
        return bisect.bisect_left(self.user_cum_weights, x)

    def pick_merchant(self, rng: random.Random) -> str:
        x = rng.random() * self.merchant_cum_weights[-1]
        return self.merchants[bisect.bisect_left(self.merchant_cum_weights, x)]

    def pick_agent(self, rng: random.Random, district: str) -> str:
        return rng.choice(self.agents_by_district[district])

    def contact_of(self, rng: random.Random, user_idx: int) -> str:
        """P2P receivers come from a small, stable contact list per user (8 contacts)."""
        slot = rng.randrange(8)
        contact = (user_idx * 2654435761 + slot * 40503) % len(self.users)
        if contact == user_idx:
            contact = (contact + 1) % len(self.users)
        return self.users[contact]

    @staticmethod
    def device_of(user_id: str) -> str:
        """A user's usual device. Fraud scenarios use a different one."""
        return f"D{user_id[1:]}"
