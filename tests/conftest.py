from __future__ import annotations

import json
from typing import Any

import pytest

from streamplat.settings import CONTRACTS


@pytest.fixture
def v1() -> dict[str, Any]:
    return json.loads((CONTRACTS / "v1.avsc").read_text())


@pytest.fixture
def v2() -> dict[str, Any]:
    return json.loads((CONTRACTS / "v2.avsc").read_text())


def breaking(name: str) -> dict[str, Any]:
    return json.loads((CONTRACTS / "breaking" / f"{name}.avsc").read_text())
