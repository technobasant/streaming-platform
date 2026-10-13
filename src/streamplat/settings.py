"""Connection settings and topic names, from the environment.

Defaults match the compose network, where every tool runs as a container.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

RAW_TOPIC = "clickstream.raw"
VALID_TOPIC = "events.valid"
DLQ_TOPIC = "events.dlq"
CLEAN_TOPIC = "events.clean"
LATE_TOPIC = "events.late"
WINDOWS_TOPIC = "agg.windows"
SESSIONS_TOPIC = "agg.sessions"

SUBJECT = f"{RAW_TOPIC}-value"
# The repository's contracts/ directory; the tools image sets CONTRACTS_DIR.
CONTRACTS = Path(
    os.environ.get(
        "CONTRACTS_DIR", Path(__file__).resolve().parents[2] / "contracts" / "clickstream"
    )
)


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    bootstrap: str = _env("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    registry_url: str = _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    clickhouse_host: str = _env("CLICKHOUSE_HOST", "clickhouse")
    clickhouse_port: int = int(_env("CLICKHOUSE_PORT", "8123"))
    clickhouse_user: str = _env("CLICKHOUSE_USER", "default")
    clickhouse_password: str = _env("CLICKHOUSE_PASSWORD", "lumen")
    iceberg_uri: str = _env("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    s3_endpoint: str = _env("S3_ENDPOINT", "http://minio:9000")
    s3_access_key: str = _env("AWS_ACCESS_KEY_ID", "lumen")
    s3_secret_key: str = _env("AWS_SECRET_ACCESS_KEY", "lumen-lab-only")


def contract_path(name: str = "v1.avsc") -> Path:
    return CONTRACTS / name


def load_contract(name: str = "v1.avsc") -> str:
    return contract_path(name).read_text()
