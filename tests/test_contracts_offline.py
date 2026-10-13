"""Avro schema resolution without a registry: the same rules the registry enforces.

FULL compatibility means old readers read new data and new readers read old data.
Each breaking schema must fail one of the two directions.
"""

from __future__ import annotations

from typing import Any

import fastavro
import pytest
from conftest import breaking

from streamplat import wire
from streamplat.events import Faults, schedule

RECORD: dict[str, Any] = {
    "event_id": "r-000000001",
    "event_type": "PAYMENT_SUCCEEDED",
    "event_time": 1_790_000_000_000,
    "session_id": "r-s0000001",
    "customer_id": None,
    "client": "WEB",
    "page": None,
    "product_id": None,
    "quantity": None,
    "order_id": "r-o0000001",
    "amount_cents": 4599,
    "currency": "EUR",
    "is_probe": False,
}


def test_new_reader_reads_old_data(v1, v2):
    out = wire.decode(v1, wire.encode(v1, RECORD), reader=v2)
    assert out["referrer"] is None
    assert out["amount_cents"] == 4599


def test_old_reader_reads_new_data(v1, v2):
    out = wire.decode(v2, wire.encode(v2, {**RECORD, "referrer": "newsletter"}), reader=v1)
    assert "referrer" not in out
    assert out["order_id"] == "r-o0000001"


def test_required_field_without_default_breaks_backward(v1):
    with pytest.raises(fastavro.read.SchemaResolutionError):
        wire.decode(v1, wire.encode(v1, RECORD), reader=breaking("add-required-field"))


def test_type_change_breaks_both_directions(v1):
    changed = breaking("change-amount-type")
    with pytest.raises(fastavro.read.SchemaResolutionError):
        wire.decode(v1, wire.encode(v1, RECORD), reader=changed)
    with pytest.raises(fastavro.read.SchemaResolutionError):
        wire.decode(changed, wire.encode(changed, {**RECORD, "amount_cents": "45.99"}), reader=v1)


def test_removing_a_required_field_breaks_forward(v1):
    removed = breaking("remove-session-id")
    data = {k: v for k, v in RECORD.items() if k != "session_id"}
    with pytest.raises(fastavro.read.SchemaResolutionError):
        wire.decode(removed, wire.encode(removed, data), reader=v1)


def test_every_generated_record_conforms_to_v1(v1):
    parsed = fastavro.parse_schema(v1)
    plan = schedule("t", seed=3, faults=Faults(poison=0.05))
    for _ in range(3000):
        em = next(plan)
        if em.record is None:
            continue
        em.record["event_time"] = 1_790_000_000_000
        assert fastavro.validate(em.record, parsed, raise_errors=True)
