"""The Confluent wire format: magic byte 0, a 4-byte big-endian schema id, the Avro body.

The serializers in confluent-kafka do this for well-formed records. The generator
needs it by hand to build poison messages, and the gatekeeper needs it to say *why*
a message is poison before handing it to the deserializer.
"""

from __future__ import annotations

import io
import struct
from typing import Any

import fastavro

MAGIC = 0
HEADER = struct.Struct(">bI")


def frame(schema_id: int, body: bytes) -> bytes:
    return HEADER.pack(MAGIC, schema_id) + body


def parse_header(raw: bytes | None) -> int | None:
    """The schema id, or None when the bytes are not Confluent-framed."""
    if raw is None or len(raw) < HEADER.size:
        return None
    magic, schema_id = HEADER.unpack_from(raw)
    return schema_id if magic == MAGIC else None


def encode(schema: dict[str, Any], record: dict[str, Any]) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(schema), record)
    return buf.getvalue()


def decode(writer: dict[str, Any], body: bytes, reader: dict[str, Any] | None = None) -> Any:
    """Read ``body`` written with ``writer``, resolved to ``reader`` (Avro schema resolution)."""
    parsed_writer = fastavro.parse_schema(writer)
    parsed_reader = fastavro.parse_schema(reader) if reader is not None else None
    return fastavro.schemaless_reader(io.BytesIO(body), parsed_writer, parsed_reader)
