"""POST /v1/decisions: the gateway's synchronous risk check.

Budget: the whole call should stay well inside 50 ms at p99. One Redis round trip (pipelined),
no other I/O on the request path; the decision log is produced asynchronously. If Redis does not
answer within the store timeout the service still answers, in degraded mode (see policy.py).
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
    multiprocess,
)
from pydantic import BaseModel, Field
from redis.exceptions import RedisError

from paystream_decision import features as feat
from paystream_decision.model import Registry, score_decision
from paystream_decision.policy import Policy, decide, live_flags
from paystream_decision.store import OnlineStore

LATENCY = Histogram("decision_latency_seconds", "Time to decide, server side",
                    buckets=(0.0005, 0.001, 0.002, 0.004, 0.008, 0.016, 0.025, 0.05, 0.1, 0.25))
STORE_LATENCY = Histogram("decision_store_latency_seconds", "Online store round trip",
                          buckets=(0.0005, 0.001, 0.002, 0.004, 0.008, 0.016, 0.025, 0.05))
OUTCOMES = Counter("decisions_total", "Decisions by outcome", ["outcome"])
DEGRADED = Counter("decisions_degraded_total", "Decisions made without online features")
MODEL_LATENCY = Histogram("decision_model_latency_seconds", "Champion and challenger scoring",
                          buckets=(0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.004, 0.008))


class PaymentRequest(BaseModel):
    txn_id: str
    txn_type: str = Field(pattern="^(P2P|CASH_IN|CASH_OUT|MERCHANT_PAY|AIRTIME)$")
    sender_id: str
    receiver_id: str
    amount_paisa: int = Field(gt=0)
    device_id: str | None = None
    district: str | None = None
    channel: str | None = None
    event_time: int | None = Field(None, description="epoch millis; default: now")


class DecisionResponse(BaseModel):
    decision_id: str
    txn_id: str
    outcome: str
    reasons: list[str]
    risk_score: float
    policy_version: str
    degraded: bool
    latency_us: int


def create_app(redis, log, policy: Policy | None = None, store_timeout_s: float = 0.025,
               models: Registry | None = None) -> FastAPI:
    """``models``: the champion scores every payment with features and holds it for review above its
    threshold; the challenger is scored on the same payment and only logged."""
    policy = policy or Policy()
    store = OnlineStore(redis)

    @asynccontextmanager
    async def lifespan(app):
        yield
        log.close()

    app = FastAPI(title="PayStream decision service", version="1.0", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz():
        champion = models.champion if models else None
        return {"ok": True, "policy": policy.version, "model": champion.version if champion else None}

    @app.get("/metrics")
    async def metrics():
        registry = REGISTRY
        if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):  # several uvicorn workers: aggregate all of them
            registry = CollectorRegistry()
            multiprocess.MultiProcessCollector(registry)
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/v1/features")
    async def feature_catalog():
        return feat.FEATURES

    @app.post("/v1/decisions", response_model=DecisionResponse)
    async def decision(body: PaymentRequest):
        started = time.perf_counter()
        req = body.model_dump()
        now = req["event_time"] or int(time.time() * 1000)
        degraded = False
        t0 = time.perf_counter()
        try:
            sender, receiver, device, known, flags = await asyncio.wait_for(store.fetch(req), store_timeout_s)
            f = feat.compute(req, sender, receiver, device, known, now)
        except (TimeoutError, OSError, RedisError) as e:
            degraded, f, flags = True, None, {}
            DEGRADED.inc()
            req["_degraded_reason"] = type(e).__name__
        STORE_LATENCY.observe(time.perf_counter() - t0)

        d = decide(policy, req, f, flags, now)
        t1 = time.perf_counter()
        scores = score_decision(models, f, d)
        if scores:
            MODEL_LATENCY.observe(time.perf_counter() - t1)
        latency_us = int((time.perf_counter() - started) * 1_000_000)
        LATENCY.observe(latency_us / 1_000_000)
        OUTCOMES.labels(d.outcome).inc()

        result = {
            "decision_id": str(uuid.uuid4()),
            "txn_id": req["txn_id"],
            "outcome": d.outcome,
            "reasons": d.reasons,
            "risk_score": round(d.risk_score, 4),
            "policy_version": policy.version,
            "degraded": degraded,
            "latency_us": latency_us,
        }
        log({**result, "decided_at": int(time.time() * 1000), "event_time": now,
             "request": {k: v for k, v in req.items() if not k.startswith("_")},
             "features": f, "flags": sorted({r for fl in flags.values() for r in live_flags(fl, now)}), **scores})
        return result

    return app


def app_from_env() -> FastAPI:
    """uvicorn paystream_decision.api:app_from_env --factory"""
    import redis.asyncio as aioredis

    from paystream_decision.log import KafkaDecisionLog, MemoryDecisionLog

    r = aioredis.Redis(host=os.environ.get("REDIS_HOST", "localhost"), port=int(os.environ.get("REDIS_PORT", "6379")),
                       password=os.environ.get("REDIS_PASSWORD") or None, ssl=os.environ.get("REDIS_TLS") == "true",
                       socket_timeout=0.05, max_connections=int(os.environ.get("REDIS_POOL", "64")))
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP")
    model_dir = os.environ.get("MODEL_REGISTRY")
    return create_app(r, KafkaDecisionLog(bootstrap) if bootstrap else MemoryDecisionLog(),
                      store_timeout_s=float(os.environ.get("STORE_TIMEOUT_MS", "25")) / 1000,
                      models=Registry(model_dir) if model_dir else None)
