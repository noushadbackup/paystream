"""Open-loop load test for the decision API.

Requests are sent on a fixed schedule whatever the latency (open loop), so a slow server shows up
as latency instead of quietly lowering the request rate. Payments come from the PayStream generator
when it is installed, so they hit the same accounts and devices Flink has features for.

  paystream-decision-loadtest --url http://localhost:8095 --rps 2000 --seconds 60 --out run.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import random
import time
from collections import Counter


def requests_from_generator(seed: int):
    try:
        from paystream_gen.population import Population
        from paystream_gen.traffic import Config, Simulator
    except ImportError:
        return None
    pop = Population.build(200_000, 20_000, 5_000, seed=seed)  # same population as paystream-gen defaults
    sim = Simulator(pop, Config(rate_per_s=1000, fraud_scale=1), seed=seed + 1)
    for e in sim.run(int(time.time() * 1000), 10**9):
        ev = e.event
        yield {k: ev[k] for k in ("txn_id", "txn_type", "sender_id", "receiver_id", "amount_paisa", "device_id",
                                  "district")} | {"channel": ev.get("channel")}


def synthetic(seed: int):
    rng = random.Random(seed)
    i = 0
    while True:
        i += 1
        yield {"txn_id": f"L{i}", "txn_type": rng.choice(["P2P", "CASH_OUT", "MERCHANT_PAY"]),
               "sender_id": f"U{rng.randrange(200_000):07d}", "receiver_id": f"U{rng.randrange(200_000):07d}",
               "amount_paisa": rng.randrange(1_000, 2_000_000), "device_id": f"D{rng.randrange(200_000):07d}"}


def pct(xs: list[float], q: float) -> float:
    s = sorted(xs)
    return round(s[min(len(s) - 1, int(q * len(s)))], 3) if s else float("nan")


async def _client(url: str, bodies: list[dict], rps: float, seconds: int, max_in_flight: int) -> dict:
    import aiohttp

    client_ms, server_us, outcomes, errors = [], [], Counter(), Counter()
    in_flight = asyncio.Semaphore(max_in_flight)
    dropped = 0
    source = iter(bodies)
    conn = aiohttp.TCPConnector(limit=max_in_flight)
    async with aiohttp.ClientSession(base_url=url, connector=conn, timeout=aiohttp.ClientTimeout(total=2)) as http:
        async def one(body: dict) -> None:
            t0 = time.perf_counter()
            try:
                async with http.post("/v1/decisions", json=body) as r:
                    r.raise_for_status()
                    d = await r.json()
                client_ms.append((time.perf_counter() - t0) * 1000)
                server_us.append(d["latency_us"])
                outcomes[d["outcome"]] += 1
            except Exception as e:  # noqa: BLE001 - every failure is counted, none stops the run
                errors[type(e).__name__] += 1
            finally:
                in_flight.release()

        tasks, start, n = [], time.perf_counter(), 0
        while (elapsed := time.perf_counter() - start) < seconds:
            due = int(elapsed * rps)
            while n < due:
                n += 1
                if in_flight.locked():  # the client itself is saturated: count it, do not hide it
                    dropped += 1
                    continue
                await in_flight.acquire()
                tasks.append(asyncio.create_task(one(next(source))))
            await asyncio.sleep(0.0005)
        await asyncio.gather(*tasks)
    return {"client_ms": client_ms, "server_us": server_us, "outcomes": outcomes, "errors": errors,
            "dropped": dropped, "wall": time.perf_counter() - start}


def _worker(args) -> dict:
    return asyncio.run(_client(*args))


def run(url: str, rps: int, seconds: int, seed: int, max_in_flight: int, procs: int) -> dict:
    """Splits the target rate across `procs` client processes, so the client is not the bottleneck."""
    from concurrent.futures import ProcessPoolExecutor

    # Built before the clock starts, so generating payments never competes with sending them.
    gen = requests_from_generator(seed) or synthetic(seed)
    bodies = [next(gen) for _ in range(rps * seconds)]
    shards = [(url, bodies[i::procs], rps / procs, seconds, max_in_flight) for i in range(procs)]
    with ProcessPoolExecutor(procs) as pool:
        parts = list(pool.map(_worker, shards))

    client_ms = [x for p in parts for x in p["client_ms"]]
    server_us = [x for p in parts for x in p["server_us"]]
    wall = max(p["wall"] for p in parts)
    return {
        "target_rps": rps,
        "seconds": seconds,
        "client_processes": procs,
        "achieved_rps": round(len(client_ms) / wall, 1),
        "requests_ok": len(client_ms),
        "errors": dict(sum((p["errors"] for p in parts), Counter())),
        "not_sent_client_saturated": sum(p["dropped"] for p in parts),
        "client_latency_ms": {q: pct(client_ms, v) for q, v in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))},
        "server_latency_ms": {q: pct([u / 1000 for u in server_us], v)
                              for q, v in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))},
        "outcomes": dict(sum((p["outcomes"] for p in parts), Counter())),
        "machine": {"python": platform.python_version(), "platform": platform.platform(),
                    "cpus": os.cpu_count()},
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:8095")
    p.add_argument("--rps", type=int, default=1000)
    p.add_argument("--seconds", type=int, default=60)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--max-in-flight", type=int, default=256, help="per client process")
    p.add_argument("--procs", type=int, default=2, help="client processes")
    p.add_argument("--out")
    a = p.parse_args()
    report = run(a.url, a.rps, a.seconds, a.seed, a.max_in_flight, a.procs)
    text = json.dumps(report, indent=2)
    print(text)
    if a.out:
        with open(a.out, "w") as f:
            f.write(text + "\n")


if __name__ == "__main__":
    main()
