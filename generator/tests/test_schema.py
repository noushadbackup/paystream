"""Every generated event must encode against the Avro contract, in both schema versions."""

import io
import json
from pathlib import Path

import fastavro

from paystream_gen.population import Population
from paystream_gen.traffic import Config, Simulator

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"


def load(name):
    return fastavro.parse_schema(json.loads((SCHEMAS / name).read_text()))


def test_events_encode_against_contract():
    pop = Population.build(1_000, 50, 30, seed=3)
    for version, schema in [(1, load("history/payment_event.v1.avsc")), (2, load("payment_event.avsc"))]:
        events = [e.event for e in Simulator(pop, Config(rate_per_s=100, fraud_scale=10,
                                                         schema_version=version), seed=3).run(0, 60)]
        buf = io.BytesIO()
        fastavro.writer(buf, schema, events, validator=True)
        buf.seek(0)
        assert [r["txn_id"] for r in fastavro.reader(buf)] == [e["txn_id"] for e in events]


def test_v2_reader_reads_v1_data_with_null_channel():
    """BACKWARD compatibility: consumers on v2 can read events written by v1 producers."""
    v1, v2 = load("history/payment_event.v1.avsc"), load("payment_event.avsc")
    pop = Population.build(1_000, 50, 30, seed=3)
    events = [e.event for e in Simulator(pop, Config(rate_per_s=20, schema_version=1), seed=3).run(0, 5)]
    buf = io.BytesIO()
    fastavro.writer(buf, v1, events)
    buf.seek(0)
    assert all(r["channel"] is None for r in fastavro.reader(buf, reader_schema=v2))


def test_alert_v2_reader_reads_v1_alerts():
    """Alerts v2 adds analyst-rule fields; alerts written before it still read, as built-in, live rules."""
    v1, v2 = load("history/fraud_alert.v1.avsc"), load("fraud_alert.avsc")
    alert = {"alert_id": "VELOCITY:U1:T1", "rule": "VELOCITY", "account_id": "U1", "trigger_txn_id": "T1",
             "event_time": 0, "ingest_time": 0, "detected_at": 5, "score": 1.2, "details": "6 outgoing"}
    buf = io.BytesIO()
    fastavro.writer(buf, v1, [alert])
    buf.seek(0)
    [r] = list(fastavro.reader(buf, reader_schema=v2))
    assert r["rule"] == "VELOCITY" and r["rule_id"] is None and r["shadow"] is False and r["action"] == "REVIEW"


def test_label_v2_reader_reads_v1_labels_as_simulator_fraud():
    v1, v2 = load("history/fraud_label.v1.avsc"), load("fraud_label.avsc")
    buf = io.BytesIO()
    fastavro.writer(buf, v1, [{"txn_id": "T1", "scenario": "mule_ring", "account_id": "U1"}])
    buf.seek(0)
    [r] = list(fastavro.reader(buf, reader_schema=v2))
    assert (r["source"], r["disposition"], r["case_id"]) == ("simulator", "FRAUD", None)
