"""HTTP API for the fraud operations team.

  GET  /cases                       the open queue, most urgent first
  POST /cases/claim                 take the next open case (two analysts never get the same one)
  GET  /cases/{id}                  a case with its alerts and full history
  POST /cases/{id}/notes            add a note
  POST /cases/{id}/close            verdict: CONFIRMED_FRAUD, FALSE_POSITIVE or INCONCLUSIVE
  POST /accounts/{id}/unfreeze      lift a freeze (lead)
  GET  /stats                       queue depth, SLA breaches, verdicts, pending effects

Callers authenticate with a bearer token mapped to a user and role (PAYSTREAM_CASES_TOKENS).
"""

from __future__ import annotations

import logging
import os
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from paystream_cases.store import CaseError, CaseStore, NotFound

ROLES = {"analyst": {"work"}, "lead": {"work", "unfreeze"}, "admin": {"work", "unfreeze"}}
log = logging.getLogger("paystream.cases")


class User(BaseModel):
    name: str
    role: str

    def can(self, permission: str) -> bool:
        return permission in ROLES.get(self.role, set())


class Note(BaseModel):
    text: str


class Verdict(BaseModel):
    verdict: str
    note: str = ""


class Unfreeze(BaseModel):
    reason: str


def parse_tokens(spec: str) -> dict[str, User]:
    """'tok1=alice:analyst,tok2=bob:lead' -> {token: User}"""
    out = {}
    for item in filter(None, (s.strip() for s in spec.split(","))):
        token, who = item.split("=", 1)
        name, role = who.split(":", 1)
        out[token] = User(name=name, role=role)
    return out


def current_user(request: Request, authorization: Annotated[str | None, Header()] = None) -> User:
    token = (authorization or "").removeprefix("Bearer ").strip()
    if token not in request.app.state.tokens:
        raise HTTPException(401, "unknown or missing bearer token")
    return request.app.state.tokens[token]


Who = Annotated[User, Depends(current_user)]


def create_app(store: CaseStore, tokens: dict[str, User], effects=None) -> FastAPI:
    """``effects`` applies outbox entries (see actions.Effects). Closing a case dispatches right away so
    a freeze is live in the same request; the consumer retries anything that failed."""
    app = FastAPI(title="PayStream case service", version="1.0")
    app.state.tokens = tokens

    def need(u: User, permission: str) -> None:
        if not u.can(permission):
            raise HTTPException(403, f"{u.role} cannot {permission}")

    def dispatch() -> int:
        if effects is None:
            return 0
        try:
            return store.dispatch(effects)
        except Exception:  # the database already holds the decision; the consumer will retry
            log.exception("dispatching case effects failed")
            return 0

    def conflict(e: CaseError) -> HTTPException:
        return HTTPException(404 if isinstance(e, NotFound) else 409, str(e))

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/cases")
    def queue(u: Who, limit: int = 50):
        need(u, "work")
        return store.queue(limit)

    @app.post("/cases/claim")
    def claim(u: Who):
        need(u, "work")
        case = store.claim_next(u.name)
        if case is None:
            raise HTTPException(404, "the queue is empty")
        return case

    @app.get("/cases/{case_id}")
    def get_case(case_id: str, u: Who):
        need(u, "work")
        try:
            return store.case(case_id)
        except CaseError as e:
            raise conflict(e) from e

    @app.post("/cases/{case_id}/notes", status_code=201)
    def note(case_id: str, body: Note, u: Who):
        need(u, "work")
        try:
            store.note(u.name, case_id, body.text)
        except CaseError as e:
            raise conflict(e) from e
        return {"ok": True}

    @app.post("/cases/{case_id}/close")
    def close(case_id: str, body: Verdict, u: Who):
        need(u, "work")
        try:
            closure = store.close(u.name, case_id, body.verdict, body.note)
        except CaseError as e:
            raise HTTPException(422 if "verdict must be" in str(e) else 404 if isinstance(e, NotFound)
                                else 409, str(e)) from e
        dispatch()
        return {"case_id": closure.case_id, "verdict": closure.verdict, "labels": len(closure.labels),
                "frozen": closure.freeze, "cleared": closure.clear_flags,
                "effects_pending": store.outbox_backlog()}

    @app.post("/accounts/{subject_id}/unfreeze")
    def unfreeze(subject_id: str, body: Unfreeze, u: Who):
        need(u, "unfreeze")
        try:
            store.unfreeze(u.name, subject_id, body.reason)
        except CaseError as e:
            raise HTTPException(404, str(e)) from e
        dispatch()
        return {"subject_id": subject_id, "effects_pending": store.outbox_backlog()}

    @app.get("/stats")
    def stats(u: Who):
        need(u, "work")
        return store.stats()

    return app


def effects_from_env():
    """Kafka labels and Redis account actions, or None when KAFKA_BOOTSTRAP is not set (local demo)."""
    from pathlib import Path

    from paystream_cases.actions import AccountActions, Effects, KafkaLabels, MemoryLabels

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP")
    labels = (KafkaLabels(bootstrap, os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
                          Path(os.environ.get("PAYSTREAM_LABEL_SCHEMA", "/app/schemas/fraud_label.avsc")))
              if bootstrap else MemoryLabels())
    accounts = None
    if os.environ.get("REDIS_HOST"):
        import redis

        accounts = AccountActions(redis.Redis(host=os.environ["REDIS_HOST"],
                                              port=int(os.environ.get("REDIS_PORT", "6379"))))
    return Effects(labels, accounts)


def app_from_env() -> FastAPI:
    """uvicorn paystream_cases.api:app_from_env --factory"""
    store = CaseStore(os.environ.get("CASES_DB", "sqlite:///cases.db"))
    return create_app(store, parse_tokens(os.environ.get("PAYSTREAM_CASES_TOKENS", "")), effects_from_env())
