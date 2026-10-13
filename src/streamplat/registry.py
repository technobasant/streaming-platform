"""Schema Registry over its REST API, plus an encoder that refuses unregistered schemas.

The confluent-kafka serializers would do the same with ``auto.register.schemas=false``;
doing it here in a few lines keeps the contract behaviour visible and testable.
"""

from __future__ import annotations

import io
import json
from functools import lru_cache
from typing import Any

import fastavro
import httpx

from . import wire

CT = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


class RegistryError(RuntimeError):
    def __init__(self, status: int, code: int | None, message: str) -> None:
        super().__init__(f"HTTP {status} (error_code {code}): {message}")
        self.status = status
        self.code = code
        self.message = message


class Registry:
    def __init__(self, url: str, timeout: float = 10.0) -> None:
        self.http = httpx.Client(base_url=url, timeout=timeout, headers=CT)

    def _call(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        r = self.http.request(method, path, json=body)
        if r.status_code >= 400:
            try:
                err = r.json()
            except ValueError:
                err = {"message": r.text}
            raise RegistryError(r.status_code, err.get("error_code"), str(err.get("message")))
        return r.json()

    def set_compatibility(self, subject: str, level: str) -> str:
        return str(
            self._call("PUT", f"/config/{subject}", {"compatibility": level})["compatibility"]
        )

    def compatibility(self, subject: str) -> str:
        return str(self._call("GET", f"/config/{subject}")["compatibilityLevel"])

    def register(self, subject: str, schema: str) -> int:
        return int(self._call("POST", f"/subjects/{subject}/versions", {"schema": schema})["id"])

    def lookup(self, subject: str, schema: str) -> int:
        """The id of ``schema`` under ``subject``; raises if it was never registered there."""
        return int(self._call("POST", f"/subjects/{subject}", {"schema": schema})["id"])

    def check(self, subject: str, schema: str) -> tuple[bool, list[str]]:
        """Would ``schema`` be accepted as the next version? (verbose: the reasons it would not)"""
        body = self._call(
            "POST",
            f"/compatibility/subjects/{subject}/versions/latest?verbose=true",
            {"schema": schema},
        )
        return bool(body["is_compatible"]), list(body.get("messages", []))

    def schema_by_id(self, schema_id: int) -> str:
        return str(self._call("GET", f"/schemas/ids/{schema_id}")["schema"])

    def versions(self, subject: str) -> list[int]:
        return [int(v) for v in self._call("GET", f"/subjects/{subject}/versions")]


class Encoder:
    """Avro-encode records under a schema that must already be registered for ``subject``.

    Construction fails when it is not: a producer built against an unapproved schema
    never sends a byte.
    """

    def __init__(self, registry: Registry, subject: str, schema: str) -> None:
        self.schema_id = registry.lookup(subject, schema)
        self.parsed = fastavro.parse_schema(json.loads(schema))

    def encode(self, record: dict[str, Any]) -> bytes:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self.parsed, record)
        return wire.frame(self.schema_id, buf.getvalue())


class Decoder:
    """Decode Confluent-framed Avro written under any registered schema, resolved to ``reader``."""

    def __init__(self, registry: Registry, reader: str) -> None:
        self.registry = registry
        self.reader = fastavro.parse_schema(json.loads(reader))
        self._writer = lru_cache(maxsize=64)(self._fetch)

    def _fetch(self, schema_id: int) -> Any:
        return fastavro.parse_schema(json.loads(self.registry.schema_by_id(schema_id)))

    def decode(self, raw: bytes) -> dict[str, Any]:
        schema_id = wire.parse_header(raw)
        if schema_id is None:
            raise ValueError("not Confluent-framed")
        writer = self._writer(schema_id)
        body = io.BytesIO(raw[wire.HEADER.size :])
        record = fastavro.schemaless_reader(body, writer, self.reader)
        if body.tell() != len(raw) - wire.HEADER.size:
            raise ValueError("trailing bytes after the record")
        if not isinstance(record, dict):
            raise ValueError("not a record")
        return record
