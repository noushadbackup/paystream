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
