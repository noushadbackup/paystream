"""Versioned rule store with maker-checker approval and a tamper-evident audit log.

Lifecycle of a version:  DRAFT -> PENDING -> PUBLISHED -> SUPERSEDED
                                         \\-> REJECTED
A published version is immutable; a change is a new version. Whoever wrote a version cannot approve
it. Every state change appends to the audit log in the same database transaction, and each audit
row carries a SHA-256 over the previous row's hash and its own content, so editing or deleting a
past row breaks the chain and `verify_audit()` reports where.

Publishing happens inside the transaction: if Kafka refuses the rule, nothing is recorded as
published. If the commit fails after Kafka accepted it, `republish()` (run at startup) re-sends every
published version; Flink ignores versions it already has.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    func,
    insert,
    select,
    text,
    update,
)
from sqlalchemy.engine import Connection, Engine

from paystream_rules.rule import validate

metadata = MetaData(schema=None)

rule_versions = Table(
    "rule_versions", metadata,
    Column("rule_id", String(64), primary_key=True),
    Column("version", Integer, primary_key=True),
    Column("body", JSON, nullable=False),
    Column("status", String(16), nullable=False),
    Column("author", String(64), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("reviewer", String(64)),
    Column("reviewed_at", DateTime(timezone=True)),
    Column("review_comment", Text),
    Column("backtest", JSON),
)

audit_log = Table(
    "audit_log", metadata,
    Column("seq", Integer, primary_key=True, autoincrement=True),
    Column("at", DateTime(timezone=True), nullable=False),
    Column("actor", String(64), nullable=False),
    Column("action", String(32), nullable=False),
    Column("rule_id", String(64)),
    Column("version", Integer),
    Column("detail", JSON),
    Column("prev_hash", String(64), nullable=False),
    Column("hash", String(64), nullable=False),
)

GENESIS = "0" * 64
Publisher = Callable[[dict], None]


class WorkflowError(Exception):
    """The request is valid but not allowed in this state, or not by this person."""


class NotFound(Exception):
    pass


def _now() -> datetime:
    return datetime.now(UTC)


def _digest(prev: str, row: dict) -> str:
    fields = {k: row[k] for k in ("actor", "action", "rule_id", "version", "detail")}
    fields["at"] = _as_utc(row["at"]).astimezone(UTC).isoformat()  # same text whatever the session time zone
    body = json.dumps(fields, sort_keys=True, default=str)
    return hashlib.sha256((prev + body).encode()).hexdigest()


class RuleStore:
    def __init__(self, url_or_engine: str | Engine, publish: Publisher):
        self.db = create_engine(url_or_engine) if isinstance(url_or_engine, str) else url_or_engine
        self.publish = publish
        metadata.create_all(self.db)

    # ------------------------------------------------------------------ reads

    def rules(self) -> list[dict]:
        """One row per rule: its latest version and what is live."""
        with self.db.connect() as c:
            rows = c.execute(select(rule_versions).order_by(rule_versions.c.rule_id, rule_versions.c.version)).all()
        out: dict[str, dict] = {}
        for r in rows:
            item = out.setdefault(r.rule_id, {"rule_id": r.rule_id, "live": None, "latest": None})
            item["latest"] = {"version": r.version, "status": r.status, "author": r.author}
            if r.status == "PUBLISHED":
                item["live"] = {"version": r.version, "state": r.body["state"], "name": r.body.get("name")}
        return list(out.values())

    def history(self, rule_id: str) -> list[dict]:
        with self.db.connect() as c:
            rows = c.execute(select(rule_versions).where(rule_versions.c.rule_id == rule_id)
                             .order_by(rule_versions.c.version)).mappings().all()
        if not rows:
            raise NotFound(rule_id)
        return [dict(r) for r in rows]

    def version(self, rule_id: str, version: int) -> dict:
        with self.db.connect() as c:
            return dict(self._get(c, rule_id, version))

    def published(self) -> list[dict]:
        with self.db.connect() as c:
            return [r.body for r in c.execute(select(rule_versions.c.body)
                                              .where(rule_versions.c.status == "PUBLISHED"))]

    def audit(self) -> list[dict]:
        with self.db.connect() as c:
            return [dict(r) for r in c.execute(select(audit_log).order_by(audit_log.c.seq)).mappings()]

    def verify_audit(self) -> dict:
        prev = GENESIS
        rows = self.audit()
        for row in rows:
            if row["prev_hash"] != prev or row["hash"] != _digest(prev, row):
                return {"ok": False, "rows": len(rows), "broken_at_seq": row["seq"]}
            prev = row["hash"]
        return {"ok": True, "rows": len(rows), "head": prev}

    # ----------------------------------------------------------------- writes

    def create_draft(self, actor: str, body: dict) -> dict:
        """Validates and stores a new version. The service assigns the version number."""
        body = {k: v for k, v in body.items() if k != "version"}
        with self.db.begin() as c:
            latest = c.execute(select(func.max(rule_versions.c.version))
                               .where(rule_versions.c.rule_id == body.get("rule_id"))).scalar()
            body = validate({**body, "version": (latest or 0) + 1})
            c.execute(insert(rule_versions).values(rule_id=body["rule_id"], version=body["version"], body=body,
                                                   status="DRAFT", author=actor, created_at=_now()))
            self._audit(c, actor, "CREATE_DRAFT", body["rule_id"], body["version"], {"body": body})
        return body

    def attach_backtest(self, actor: str, rule_id: str, version: int, result: dict) -> None:
        with self.db.begin() as c:
            self._get(c, rule_id, version)
            c.execute(update(rule_versions).where(rule_versions.c.rule_id == rule_id,
                                                  rule_versions.c.version == version).values(backtest=result))
            self._audit(c, actor, "BACKTEST", rule_id, version, {"summary": result.get("summary")})

    def submit(self, actor: str, rule_id: str, version: int) -> None:
        with self.db.begin() as c:
            row = self._get(c, rule_id, version)
            if row["author"] != actor:
                raise WorkflowError("only the author can submit a draft for approval")
            self._transition(c, row, "DRAFT", "PENDING")
            self._audit(c, actor, "SUBMIT", rule_id, version, None)

    def approve(self, actor: str, rule_id: str, version: int, comment: str = "") -> dict:
        """Maker-checker: a second person approves, and the rule goes live in the same step."""
        with self.db.begin() as c:
            row = self._get(c, rule_id, version)
            if row["author"] == actor:
                raise WorkflowError("four-eyes rule: the author of a version cannot approve it")
            self._transition(c, row, "PENDING", "PUBLISHED", reviewer=actor, comment=comment)
            self._supersede(c, rule_id, version)
            self._audit(c, actor, "APPROVE_PUBLISH", rule_id, version, {"comment": comment})
            self.publish(row["body"])  # raises -> transaction rolls back, nothing recorded as live
        return row["body"]

    def reject(self, actor: str, rule_id: str, version: int, comment: str) -> None:
        with self.db.begin() as c:
            row = self._get(c, rule_id, version)
            if row["author"] == actor:
                raise WorkflowError("the author of a version cannot review it")
            self._transition(c, row, "PENDING", "REJECTED", reviewer=actor, comment=comment)
            self._audit(c, actor, "REJECT", rule_id, version, {"comment": comment})

    def disable(self, actor: str, rule_id: str, reason: str) -> dict:
        """Kill switch: publishes a DISABLED version at once. Recorded as an emergency action, not a review."""
        with self.db.begin() as c:
            live = c.execute(select(rule_versions).where(rule_versions.c.rule_id == rule_id,
                                                         rule_versions.c.status == "PUBLISHED")).mappings().first()
            if live is None:
                raise NotFound(f"{rule_id} has no live version")
            latest = c.execute(select(func.max(rule_versions.c.version))
                               .where(rule_versions.c.rule_id == rule_id)).scalar()
            body = {**live["body"], "version": latest + 1, "state": "DISABLED"}
            c.execute(insert(rule_versions).values(rule_id=rule_id, version=body["version"], body=body,
                                                   status="PUBLISHED", author=actor, created_at=_now(),
                                                   reviewer=actor, reviewed_at=_now(), review_comment=reason))
            self._supersede(c, rule_id, body["version"])
            self._audit(c, actor, "EMERGENCY_DISABLE", rule_id, body["version"], {"reason": reason})
            self.publish(body)
        return body

    def bootstrap(self, body: dict) -> dict | None:
        """Loads a rule from the reviewed library in git at first start. Skipped if the rule already exists."""
        with self.db.begin() as c:
            if c.execute(select(rule_versions.c.rule_id).where(rule_versions.c.rule_id == body["rule_id"])).first():
                return None
            body = validate({**body, "version": 1})
            c.execute(insert(rule_versions).values(rule_id=body["rule_id"], version=1, body=body, status="PUBLISHED",
                                                   author="bootstrap", created_at=_now(), reviewer="git-review",
                                                   reviewed_at=_now(), review_comment="rules/library"))
            self._audit(c, "bootstrap", "BOOTSTRAP_PUBLISH", body["rule_id"], 1, {"body": body})
            self.publish(body)
        return body

    def republish(self) -> int:
        bodies = self.published()
        for b in bodies:
            self.publish(b)
        return len(bodies)

    # ---------------------------------------------------------------- helpers

    def _get(self, c: Connection, rule_id: str, version: int):
        row = c.execute(select(rule_versions).where(rule_versions.c.rule_id == rule_id,
                                                    rule_versions.c.version == version)).mappings().first()
        if row is None:
            raise NotFound(f"{rule_id} v{version}")
        return row

    def _transition(self, c: Connection, row, src: str, dst: str, reviewer: str | None = None,
                    comment: str | None = None) -> None:
        if row["status"] != src:
            raise WorkflowError(f"{row['rule_id']} v{row['version']} is {row['status']}, expected {src}")
        values = {"status": dst}
        if reviewer:
            values |= {"reviewer": reviewer, "reviewed_at": _now(), "review_comment": comment}
        c.execute(update(rule_versions).where(rule_versions.c.rule_id == row["rule_id"],
                                              rule_versions.c.version == row["version"],
                                              rule_versions.c.status == src).values(**values))

    def _supersede(self, c: Connection, rule_id: str, keep: int) -> None:
        c.execute(update(rule_versions).where(rule_versions.c.rule_id == rule_id,
                                              rule_versions.c.status == "PUBLISHED",
                                              rule_versions.c.version != keep).values(status="SUPERSEDED"))

    def _audit(self, c: Connection, actor: str, action: str, rule_id: str | None, version: int | None,
               detail: dict | None) -> None:
        if c.dialect.name == "postgresql":  # one chain: concurrent writers queue here instead of forking it
            c.execute(text("SELECT pg_advisory_xact_lock(7243)"))
        prev = c.execute(select(audit_log.c.hash).order_by(audit_log.c.seq.desc()).limit(1)).scalar() or GENESIS
        row = {"at": _now().replace(microsecond=0), "actor": actor, "action": action, "rule_id": rule_id,
               "version": version, "detail": detail}
        c.execute(insert(audit_log).values(**row, prev_hash=prev, hash=_digest(prev, row)))


def _as_utc(t: datetime) -> datetime:
    return t if t.tzinfo else t.replace(tzinfo=UTC)
