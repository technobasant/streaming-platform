"""Against a real Schema Registry (CI starts one). Skipped when none is configured."""

from __future__ import annotations

import os

import pytest

from streamplat.contracts import check
from streamplat.settings import Settings

pytestmark = [
    pytest.mark.registry,
    pytest.mark.skipif("SCHEMA_REGISTRY_URL" not in os.environ, reason="no registry"),
]


def test_registry_accepts_compatible_and_refuses_breaking():
    report = check(Settings())
    assert report["compatibility"] == "FULL_TRANSITIVE"
    assert report["v2_adds_optional_field"]["compatible"] is True
    assert set(report["breaking"]) == {
        "add-required-field",
        "change-amount-type",
        "remove-session-id",
    }
    for name, entry in report["breaking"].items():
        assert entry["compatible"] is False, name
        assert entry["register"]["status"] == 409, name
        assert entry["producer"].startswith("refused before sending"), name
    assert report["passed"] is True
