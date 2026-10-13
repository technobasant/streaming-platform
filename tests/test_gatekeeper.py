from __future__ import annotations

import json
from typing import Any

import pytest
from test_contracts_offline import RECORD

from streamplat import wire
from streamplat.gatekeeper import classify, to_json
from streamplat.registry import Decoder, RegistryError
from streamplat.settings import load_contract
from streamplat.validation import violations

NOW = RECORD["event_time"] + 1000


class FakeRegistry:
    """Schema id 1 is v1; anything else is unknown, as the real registry would say."""

    def schema_by_id(self, schema_id: int) -> str:
        if schema_id == 1:
            return load_contract("v1.avsc")
        raise RegistryError(404, 40403, "Schema not found")


@pytest.fixture
def decoder() -> Decoder:
    return Decoder(FakeRegistry(), load_contract("v1.avsc"))  # type: ignore[arg-type]


def framed(v1: dict[str, Any], record: dict[str, Any], schema_id: int = 1) -> bytes:
    return wire.frame(schema_id, wire.encode(v1, record))


def test_valid_record_passes(decoder, v1, monkeypatch):
    monkeypatch.setattr("streamplat.gatekeeper.violations", lambda r: violations(r, NOW))
    record, reason = classify(decoder, framed(v1, RECORD))
    assert reason is None and record is not None
    assert record["order_id"] == "r-o0000001"


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (None, "tombstone"),
        (b'{"event":"page_view"}', "not_confluent_avro"),
        (b"\x00\x00", "not_confluent_avro"),
    ],
)
def test_unframed_bytes(decoder, raw, reason):
    assert classify(decoder, raw) == (None, reason)


def test_unknown_schema_id(decoder, v1):
    assert classify(decoder, framed(v1, RECORD, schema_id=999_999)) == (None, "schema_not_found")


def test_truncated_body(decoder, v1):
    raw = framed(v1, RECORD)
    assert classify(decoder, raw[: len(raw) // 2]) == (None, "avro_decode_error")


def test_trailing_bytes_are_not_ignored(decoder, v1):
    assert classify(decoder, framed(v1, RECORD) + b"\x00") == (None, "avro_decode_error")


@pytest.mark.parametrize(
    ("change", "rule"),
    [
        ({"amount_cents": -1}, "payment_amount_not_positive"),
        ({"order_id": None}, "order_event_without_order_id"),
        ({"currency": "XXX"}, "currency_unknown"),
        (
            {"event_type": "ADD_TO_CART", "product_id": "P1", "quantity": 0},
            "cart_quantity_not_positive",
        ),
        ({"event_type": "PAGE_VIEW", "page": None}, "page_view_without_page"),
        ({"event_time": NOW + 3_600_000}, "event_time_in_future"),
    ],
)
def test_business_rules(decoder, v1, monkeypatch, change, rule):
    monkeypatch.setattr("streamplat.gatekeeper.violations", lambda r: violations(r, NOW))
    assert classify(decoder, framed(v1, {**RECORD, **change})) == (None, f"rule:{rule}")


def test_json_shape_is_what_flink_and_clickhouse_parse():
    out = json.loads(to_json(RECORD))
    assert out["event_type"] == "payment_succeeded"
    assert out["client"] == "web"
    # Flink's json.timestamp-format.standard=SQL and ClickHouse DateTime64(3) both read this.
    assert out["event_time"] == "2026-09-21 14:13:20.000"
    assert out["is_probe"] is False
