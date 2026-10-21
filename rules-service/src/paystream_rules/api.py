"""HTTP API for analysts and approvers.

  GET  /rules                                    rules with their live and latest versions
  GET  /rules/{id}                               every version of one rule, with reviews and backtests
  POST /rules                                    new draft version (analyst)
  POST /rules/{id}/versions/{v}/backtest         replay recent lakehouse history through the draft
  POST /rules/{id}/versions/{v}/submit           send for review (author only)
  POST /rules/{id}/versions/{v}/approve          approve and publish (approver, not the author)
  POST /rules/{id}/versions/{v}/reject           (approver)
  POST /rules/{id}/disable                       kill switch, live in seconds (approver)
  GET  /audit, GET /audit/verify                 the hash-chained audit log and its integrity check

Callers authenticate with a bearer token mapped to a user and role (PAYSTREAM_RULES_TOKENS).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from paystream_rules import backtest as bt
from paystream_rules.rule import RuleError
from paystream_rules.store import NotFound, RuleStore, WorkflowError

ROLES = {"analyst": {"draft"}, "approver": {"draft", "review"}, "admin": {"draft", "review", "audit"}}


class User(BaseModel):
    name: str
    role: str

    def can(self, permission: str) -> bool:
        return permission in ROLES.get(self.role, set())


class Review(BaseModel):
    comment: str = ""


class Disable(BaseModel):
    reason: str


class BacktestWindow(BaseModel):
    days: float = 7.0
    end: datetime | None = None


def parse_tokens(spec: str) -> dict[str, User]:
    """'tok1=alice:analyst,tok2=bob:approver' -> {token: User}"""
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


def create_app(store: RuleStore, tokens: dict[str, User], source: bt.Source | None = None) -> FastAPI:
    app = FastAPI(title="PayStream rules service", version="1.0")
    app.state.tokens = tokens

    def need(u: User, permission: str) -> None:
        if not u.can(permission):
            raise HTTPException(403, f"{u.role} cannot {permission}")

    def guard(fn, *args):
        try:
            return fn(*args)
        except RuleError as e:
            raise HTTPException(422, {"problems": e.problems}) from e
        except WorkflowError as e:
            raise HTTPException(409, str(e)) from e
        except NotFound as e:
            raise HTTPException(404, f"not found: {e}") from e

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/rules")
    def list_rules(u: Who):
        return store.rules()

    @app.get("/rules/{rule_id}")
    def rule_history(rule_id: str, u: Who):
        return guard(store.history, rule_id)

    @app.post("/rules", status_code=201)
    def draft(body: dict, u: Who):
        need(u, "draft")
        return guard(store.create_draft, u.name, body)

    @app.post("/rules/{rule_id}/versions/{version}/backtest")
    def run_backtest(rule_id: str, version: int, window: BacktestWindow, u: Who):
        need(u, "draft")
        if source is None:
            raise HTTPException(503, "no lakehouse configured for backtests")
        row = guard(store.version, rule_id, version)
        end = window.end or datetime.now(UTC)
        result = bt.run(row["body"], source, end - timedelta(days=window.days), end)
        guard(store.attach_backtest, u.name, rule_id, version, result)
        return result

    @app.post("/rules/{rule_id}/versions/{version}/submit")
    def submit(rule_id: str, version: int, u: Who):
        need(u, "draft")
        guard(store.submit, u.name, rule_id, version)
        return {"status": "PENDING"}

    @app.post("/rules/{rule_id}/versions/{version}/approve")
    def approve(rule_id: str, version: int, review: Review, u: Who):
        need(u, "review")
        row = guard(store.version, rule_id, version)
        if row["backtest"] and row["backtest"]["summary"].get("hot_key_risk"):
            raise HTTPException(409, "backtest shows a hot key: narrow the filter or the grouping before approving")
        return guard(store.approve, u.name, rule_id, version, review.comment)

    @app.post("/rules/{rule_id}/versions/{version}/reject")
    def reject(rule_id: str, version: int, review: Review, u: Who):
        need(u, "review")
        guard(store.reject, u.name, rule_id, version, review.comment)
        return {"status": "REJECTED"}

    @app.post("/rules/{rule_id}/disable")
    def disable(rule_id: str, body: Disable, u: Who):
        need(u, "review")
        return guard(store.disable, u.name, rule_id, body.reason)

    @app.get("/audit")
    def audit(u: Who):
        need(u, "audit")
        return store.audit()

    @app.get("/audit/verify")
    def verify(u: Who):
        need(u, "audit")
        return store.verify_audit()

    return app


def app_from_env() -> FastAPI:
    """uvicorn paystream_rules.api:app_from_env --factory"""
    from paystream_rules.publisher import KafkaPublisher, MemoryPublisher

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP")
    store = RuleStore(os.environ.get("RULES_DB", "sqlite:///rules.db"),
                      KafkaPublisher(bootstrap) if bootstrap else MemoryPublisher())
    store.republish()
    source = None
    if path := os.environ.get("PAYSTREAM_DUCKDB"):
        source = bt.DuckDBSource(path)
    elif host := os.environ.get("TRINO_HOST"):
        source = bt.TrinoSource(host, int(os.environ.get("TRINO_PORT", "8080")))
    return create_app(store, parse_tokens(os.environ.get("PAYSTREAM_RULES_TOKENS", "")), source)
