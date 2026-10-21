"""Cases, their alerts, and an append-only history of everything done to them.

One open case per subject (account or device): new alerts on the same subject join it instead of
creating more work. Priority decides the queue order and the SLA:

  P1  15 min  a rule asked to DECLINE, or account takeover patterns: money can leave right now
  P2   2 h    velocity, mule and agent patterns, payments held by the decision service
  P3  24 h    everything else

Closing a case with a verdict produces labels (FRAUD or NOT_FRAUD) for every alert on it, and an
account action: CONFIRMED_FRAUD freezes the subject, FALSE_POSITIVE clears the flags that were
blocking it. After a false positive, the same rule on the same subject is suppressed for a while so
analysts do not re-review a customer they just cleared.

Those effects (labels to Kafka, freezes and clearances in Redis) are written to an outbox in the
same transaction that closes the case, then dispatched. A crash between the two can delay an
effect but never lose it, and every effect is idempotent, so a retry is harmless.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    case,
    create_engine,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import StaticPool

metadata = MetaData()

cases = Table(
    "cases", metadata,
    Column("case_id", String(40), primary_key=True),
    Column("subject_id", String(64), nullable=False, index=True),
    Column("status", String(16), nullable=False, index=True),  # OPEN, IN_PROGRESS, CLOSED
    Column("priority", Integer, nullable=False),
    Column("opened_at", DateTime(timezone=True), nullable=False),
    Column("sla_due_at", DateTime(timezone=True), nullable=False),
    Column("assignee", String(64)),
    Column("max_score", Float, nullable=False, default=0.0),
    Column("rules", JSON, nullable=False),
    Column("verdict", String(24)),  # CONFIRMED_FRAUD, FALSE_POSITIVE, INCONCLUSIVE
    Column("closed_at", DateTime(timezone=True)),
    Column("closed_by", String(64)),
)

case_alerts = Table(
    "case_alerts", metadata,
    Column("alert_id", String(200), primary_key=True),
    Column("case_id", String(40), nullable=False, index=True),
    Column("rule", String(64), nullable=False),
    Column("trigger_txn_id", String(64), nullable=False),
    Column("score", Float, nullable=False),
    Column("action", String(16), nullable=False),
    Column("details", Text),
    Column("event_time", DateTime(timezone=True), nullable=False),
    Column("suppressed", Integer, nullable=False, default=0),
)

case_events = Table(
    "case_events", metadata,
    Column("seq", Integer, primary_key=True, autoincrement=True),
    Column("case_id", String(40), nullable=False, index=True),
    Column("at", DateTime(timezone=True), nullable=False),
    Column("actor", String(64), nullable=False),
    Column("kind", String(24), nullable=False),
    Column("detail", JSON),
)

outbox = Table(
    "case_outbox", metadata,
    Column("seq", Integer, primary_key=True, autoincrement=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("kind", String(16), nullable=False),  # CLOSE, UNFREEZE
    Column("payload", JSON, nullable=False),
    Column("attempts", Integer, nullable=False, default=0),
    Column("last_error", Text),
    Column("dispatched_at", DateTime(timezone=True), index=True),
)

SLA = {1: timedelta(minutes=15), 2: timedelta(hours=2), 3: timedelta(hours=24)}
P1_RULES = {"NEW_DEVICE_CASH_OUT", "failed-pin-burst"}
P2_RULES = {"VELOCITY", "MULE_FAN_IN", "AGENT_FLOAT_SPIKE", "device-farm", "cash-in-structuring", "DECISION_REVIEW"}
VERDICTS = {"CONFIRMED_FRAUD": "FRAUD", "FALSE_POSITIVE": "NOT_FRAUD", "INCONCLUSIVE": None}
SUPPRESS_AFTER_FALSE_POSITIVE = timedelta(hours=72)


class CaseError(Exception):
    pass


class NotFound(CaseError):
    pass


@dataclass(frozen=True)
class Closure:
    """What closing a case must cause outside the database."""
    case_id: str
    subject_id: str
    verdict: str
    rules: list[str]
    labels: list[dict]
    freeze: bool
    clear_flags: bool
    closed_at_ms: int


def priority_of(rule: str, action: str) -> int:
    if action == "DECLINE" or rule in P1_RULES:
        return 1
    return 2 if rule in P2_RULES else 3


def _now() -> datetime:
    return datetime.now(UTC)


def _utc(t: datetime | None) -> datetime | None:
    return t if t is None or t.tzinfo else t.replace(tzinfo=UTC)


class CaseStore:
    def __init__(self, url_or_engine: str | Engine):
        if url_or_engine == "sqlite://":  # in-memory, for tests and demos: one connection shared by threads
            url_or_engine = create_engine("sqlite://", poolclass=StaticPool,
                                          connect_args={"check_same_thread": False})
        self.db = create_engine(url_or_engine) if isinstance(url_or_engine, str) else url_or_engine
        metadata.create_all(self.db)

    # ---------------------------------------------------------------- intake

    def ingest_alert(self, a: dict, now: datetime | None = None) -> str | None:
        """Idempotent on alert_id. Returns the case the alert joined, or None if it was a replay."""
        if a.get("shadow"):
            return None
        now = now or _now()
        rule = a.get("rule_id") or a["rule"]
        action = a.get("action") or "REVIEW"
        subject = a["account_id"]
        with self.db.begin() as c:
            if c.execute(select(case_alerts.c.alert_id).where(case_alerts.c.alert_id == a["alert_id"])).first():
                return None
            cleared = self._recent_false_positive(c, subject, rule, now)
            if cleared:
                self._attach(c, cleared, a, rule, action, suppressed=True)
                self._event(c, cleared, "system", "SUPPRESSED", {"alert_id": a["alert_id"], "rule": rule})
                return cleared
            row = c.execute(select(cases).where(cases.c.subject_id == subject, cases.c.status != "CLOSED")
                            .with_for_update()).mappings().first()
            p = priority_of(rule, action)
            if row is None:
                case_id = f"C-{uuid.uuid4().hex[:12]}"
                c.execute(insert(cases).values(case_id=case_id, subject_id=subject, status="OPEN", priority=p,
                                               opened_at=now, sla_due_at=now + SLA[p], max_score=a.get("score", 0),
                                               rules=[rule]))
                self._event(c, case_id, "system", "OPENED", {"rule": rule, "priority": p})
            else:
                case_id = row["case_id"]
                values = {"max_score": max(row["max_score"], a.get("score", 0)),
                          "rules": sorted(set(row["rules"]) | {rule})}
                if p < row["priority"]:  # escalation tightens the SLA from now, never loosens it
                    values |= {"priority": p, "sla_due_at": min(_utc(row["sla_due_at"]), now + SLA[p])}
                    self._event(c, case_id, "system", "ESCALATED", {"from": row["priority"], "to": p, "rule": rule})
                c.execute(update(cases).where(cases.c.case_id == case_id).values(**values))
            try:
                self._attach(c, case_id, a, rule, action)
            except IntegrityError:  # a concurrent consumer attached it first
                return None
            self._event(c, case_id, "system", "ALERT", {"alert_id": a["alert_id"], "rule": rule})
            return case_id

    def ingest_decision(self, d: dict, now: datetime | None = None) -> str | None:
        """Payments the decision service held for review become alerts on the sender's case."""
        if d.get("outcome") != "REVIEW":
            return None
        req = d["request"]
        return self.ingest_alert({
            "alert_id": f"DECISION_REVIEW:{req['sender_id']}:{d['txn_id']}", "rule": "DECISION_REVIEW",
            "account_id": req["sender_id"], "trigger_txn_id": d["txn_id"], "score": d.get("risk_score", 0),
            "action": "REVIEW", "details": ",".join(d.get("reasons", [])),
            "event_time": d.get("event_time")}, now)

    # ---------------------------------------------------------------- work

    def queue(self, limit: int = 50) -> list[dict]:
        """Open work, most urgent first: priority, then the SLA that runs out soonest."""
        with self.db.connect() as c:
            rows = c.execute(select(cases, func.count(case_alerts.c.alert_id).label("alerts"))
                             .join(case_alerts, case_alerts.c.case_id == cases.c.case_id)
                             .where(cases.c.status != "CLOSED")
                             .group_by(cases.c.case_id)
                             .order_by(cases.c.priority, cases.c.sla_due_at).limit(limit)).mappings().all()
        return [dict(r) for r in rows]

    def claim_next(self, analyst: str, now: datetime | None = None) -> dict | None:
        now = now or _now()
        with self.db.begin() as c:
            row = c.execute(select(cases).where(cases.c.status == "OPEN")
                            .order_by(cases.c.priority, cases.c.sla_due_at).limit(1)
                            .with_for_update(skip_locked=True)).mappings().first()
            if row is None:
                return None
            c.execute(update(cases).where(cases.c.case_id == row["case_id"], cases.c.status == "OPEN")
                      .values(status="IN_PROGRESS", assignee=analyst))
            self._event(c, row["case_id"], analyst, "CLAIMED", None)
        return self.case(row["case_id"])

    def note(self, analyst: str, case_id: str, text: str) -> None:
        with self.db.begin() as c:
            self._get(c, case_id)
            self._event(c, case_id, analyst, "NOTE", {"text": text})

    def close(self, analyst: str, case_id: str, verdict: str, note: str = "",
              now: datetime | None = None) -> Closure:
        if verdict not in VERDICTS:
            raise CaseError(f"verdict must be one of {sorted(VERDICTS)}")
        now = now or _now()
        with self.db.begin() as c:
            row = self._get(c, case_id)
            if row["status"] == "CLOSED":
                raise CaseError(f"{case_id} is already closed")
            if row["status"] == "IN_PROGRESS" and row["assignee"] not in (None, analyst):
                raise CaseError(f"{case_id} is being worked by {row['assignee']}")
            c.execute(update(cases).where(cases.c.case_id == case_id)
                      .values(status="CLOSED", verdict=verdict, closed_at=now, closed_by=analyst))
            self._event(c, case_id, analyst, "CLOSED", {"verdict": verdict, "note": note})
            alerts = c.execute(select(case_alerts).where(case_alerts.c.case_id == case_id,
                                                         case_alerts.c.suppressed == 0)).mappings().all()
            disposition = VERDICTS[verdict]
            labels = [] if disposition is None else [
                {"txn_id": a["trigger_txn_id"], "scenario": a["rule"], "account_id": row["subject_id"],
                 "source": "analyst", "disposition": disposition, "case_id": case_id, "labelled_by": analyst}
                for a in alerts]
            closure = Closure(case_id, row["subject_id"], verdict, sorted(row["rules"]), labels,
                              freeze=verdict == "CONFIRMED_FRAUD", clear_flags=verdict == "FALSE_POSITIVE",
                              closed_at_ms=int(now.timestamp() * 1000))
            c.execute(insert(outbox).values(created_at=_now(), kind="CLOSE", payload=asdict(closure)))
        return closure

    def unfreeze(self, analyst: str, subject_id: str, reason: str) -> None:
        """Lift a freeze a confirmed-fraud verdict put on a subject (approvers only, at the API)."""
        with self.db.begin() as c:
            row = c.execute(select(cases.c.case_id).where(cases.c.subject_id == subject_id,
                                                          cases.c.verdict == "CONFIRMED_FRAUD")
                            .order_by(cases.c.closed_at.desc())).first()
            if row is None:
                raise CaseError(f"{subject_id} has no confirmed-fraud case to unfreeze")
            self._event(c, row[0], analyst, "UNFROZEN", {"reason": reason})
            c.execute(insert(outbox).values(created_at=_now(), kind="UNFREEZE",
                                            payload={"subject_id": subject_id, "case_id": row[0]}))

    def enforcement(self, freeze_for: timedelta, allow_for: timedelta, now: datetime | None = None) -> list[dict]:
        """The account actions that should be in force now, as CLOSE effects without labels, oldest first.

        Redis is a cache: it may restart empty or evict under memory pressure. Postgres is the record,
        so replaying these restores every live freeze and clearance with its original expiry.
        """
        now = now or _now()
        horizon = now - max(freeze_for, allow_for)
        with self.db.connect() as c:
            rows = c.execute(select(cases).where(cases.c.verdict.in_(["CONFIRMED_FRAUD", "FALSE_POSITIVE"]),
                                                 cases.c.closed_at >= horizon)
                             .order_by(cases.c.closed_at)).mappings().all()
            unfrozen = {r[0] for r in c.execute(select(case_events.c.case_id).where(case_events.c.kind == "UNFROZEN"))}
        out = []
        for r in rows:
            closed_at = _utc(r["closed_at"])
            if r["verdict"] == "CONFIRMED_FRAUD" and (r["case_id"] in unfrozen or closed_at < now - freeze_for):
                continue
            if r["verdict"] == "FALSE_POSITIVE" and closed_at < now - allow_for:
                continue
            out.append(asdict(Closure(r["case_id"], r["subject_id"], r["verdict"], sorted(r["rules"]), [],
                                      freeze=r["verdict"] == "CONFIRMED_FRAUD",
                                      clear_flags=r["verdict"] == "FALSE_POSITIVE",
                                      closed_at_ms=int(closed_at.timestamp() * 1000))))
        return out

    # ---------------------------------------------------------------- outbox

    def dispatch(self, apply: Callable[[str, dict], None], limit: int = 100) -> int:
        """Apply pending effects in order. Stops at the first failure so effects never overtake each
        other (an UNFREEZE must not land before the freeze it lifts); the next call retries it."""
        done = 0
        with self.db.begin() as c:
            rows = c.execute(select(outbox).where(outbox.c.dispatched_at.is_(None)).order_by(outbox.c.seq)
                             .limit(limit).with_for_update(skip_locked=True)).mappings().all()
            for r in rows:
                try:
                    apply(r["kind"], r["payload"])
                except Exception as e:  # noqa: BLE001 - any failure is retried later
                    c.execute(update(outbox).where(outbox.c.seq == r["seq"])
                              .values(attempts=r["attempts"] + 1, last_error=f"{type(e).__name__}: {e}"[:2000]))
                    break
                c.execute(update(outbox).where(outbox.c.seq == r["seq"])
                          .values(attempts=r["attempts"] + 1, dispatched_at=_now()))
                done += 1
        return done

    def outbox_backlog(self) -> int:
        with self.db.connect() as c:
            return c.execute(select(func.count()).where(outbox.c.dispatched_at.is_(None))).scalar() or 0

    # ---------------------------------------------------------------- reads

    def case(self, case_id: str) -> dict:
        with self.db.connect() as c:
            row = dict(self._get(c, case_id))
            row["alerts"] = [dict(a) for a in c.execute(select(case_alerts).where(case_alerts.c.case_id == case_id)
                                                        .order_by(case_alerts.c.event_time)).mappings()]
            row["history"] = [dict(e) for e in c.execute(select(case_events).where(case_events.c.case_id == case_id)
                                                         .order_by(case_events.c.seq)).mappings()]
        return row

    def stats(self, now: datetime | None = None) -> dict:
        now = now or _now()
        with self.db.connect() as c:
            open_rows = c.execute(select(cases.c.priority, cases.c.sla_due_at, cases.c.opened_at)
                                  .where(cases.c.status != "CLOSED")).all()
            closed = c.execute(select(cases.c.verdict, func.count()).where(cases.c.status == "CLOSED")
                               .group_by(cases.c.verdict)).all()
            suppressed = c.execute(select(func.sum(case((case_alerts.c.suppressed == 1, 1), else_=0)))).scalar()
            backlog = c.execute(select(func.count()).where(outbox.c.dispatched_at.is_(None))).scalar()
        by_priority = {p: sum(1 for r in open_rows if r[0] == p) for p in (1, 2, 3)}
        breached = sum(1 for r in open_rows if _utc(r[1]) < now)
        oldest = max(((now - _utc(r[2])).total_seconds() for r in open_rows), default=0)
        return {"open": len(open_rows), "open_by_priority": by_priority, "sla_breached": breached,
                "oldest_open_s": round(oldest), "closed_by_verdict": dict(closed),
                "alerts_suppressed": int(suppressed or 0), "effects_pending": int(backlog or 0)}

    # ---------------------------------------------------------------- helpers

    def _get(self, c: Connection, case_id: str):
        row = c.execute(select(cases).where(cases.c.case_id == case_id)).mappings().first()
        if row is None:
            raise NotFound(f"no case {case_id}")
        return row

    def _recent_false_positive(self, c: Connection, subject: str, rule: str, now: datetime) -> str | None:
        rows = c.execute(select(cases.c.case_id, cases.c.rules, cases.c.closed_at).where(and_(
            cases.c.subject_id == subject, cases.c.verdict == "FALSE_POSITIVE"))
            .order_by(cases.c.closed_at.desc())).all()
        for case_id, rules, closed_at in rows:
            if rule in rules and now - _utc(closed_at) < SUPPRESS_AFTER_FALSE_POSITIVE:
                return case_id
        return None

    def _attach(self, c: Connection, case_id: str, a: dict, rule: str, action: str, suppressed: bool = False) -> None:
        t = a.get("event_time")
        event_time = t if isinstance(t, datetime) else datetime.fromtimestamp((t or 0) / 1000, UTC)
        c.execute(insert(case_alerts).values(alert_id=a["alert_id"], case_id=case_id, rule=rule,
                                             trigger_txn_id=a["trigger_txn_id"], score=a.get("score", 0),
                                             action=action, details=a.get("details"), event_time=event_time,
                                             suppressed=int(suppressed)))

    def _event(self, c: Connection, case_id: str, actor: str, kind: str, detail: dict | None) -> None:
        c.execute(insert(case_events).values(case_id=case_id, at=_now(), actor=actor, kind=kind, detail=detail))
