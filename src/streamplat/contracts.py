"""The contract check, against a live Schema Registry.

Proves three things and prints them as JSON: a compatible change is accepted, each
breaking change is refused with the registry's reason, and a producer built against
an unregistered schema cannot send.
"""

from __future__ import annotations

import time
from typing import Any

from .registry import Encoder, Registry, RegistryError
from .settings import CONTRACTS, Settings

SCRATCH = "contract-check-{}-value"


def check(settings: Settings) -> dict[str, Any]:
    reg = Registry(settings.registry_url)
    # A scratch subject per run, so the check never registers v2 on the live subject.
    subject = SCRATCH.format(int(time.time() * 1000))
    reg.set_compatibility(subject, "FULL_TRANSITIVE")
    v1 = (CONTRACTS / "v1.avsc").read_text()
    v2 = (CONTRACTS / "v2.avsc").read_text()
    reg.register(subject, v1)
    report: dict[str, Any] = {"subject": subject, "compatibility": reg.compatibility(subject)}

    ok, reasons = reg.check(subject, v2)
    report["v2_adds_optional_field"] = {"compatible": ok, "messages": reasons}

    breaking: dict[str, Any] = {}
    for path in sorted((CONTRACTS / "breaking").glob("*.avsc")):
        schema = path.read_text()
        ok, reasons = reg.check(subject, schema)
        entry: dict[str, Any] = {"compatible": ok, "messages": reasons[:2]}
        t0 = time.perf_counter()
        try:
            reg.register(subject, schema)
            entry["register"] = "ACCEPTED"
        except RegistryError as e:
            entry["register"] = {"status": e.status, "message": e.message[:160]}
        entry["register_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        try:
            Encoder(reg, subject, schema)
            entry["producer"] = "COULD SEND"
        except RegistryError as e:
            entry["producer"] = f"refused before sending: HTTP {e.status} ({e.code})"
        breaking[path.stem] = entry
    report["breaking"] = breaking
    report["v2_registers"] = reg.register(subject, v2) > 0
    report["versions"] = reg.versions(subject)
    report["passed"] = (
        report["v2_adds_optional_field"]["compatible"]
        and all(
            not b["compatible"]
            and isinstance(b["register"], dict)
            and b["register"]["status"] == 409
            for b in breaking.values()
        )
        and len(breaking) >= 3
    )
    reg.http.delete(f"/subjects/{subject}")
    reg.http.delete(f"/subjects/{subject}?permanent=true")
    return report
