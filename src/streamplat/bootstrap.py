"""Create the topics and register the contract. Idempotent: safe to run on every `make up`."""

from __future__ import annotations

import logging
import os
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from .registry import Registry, RegistryError
from .settings import (
    CLEAN_TOPIC,
    DLQ_TOPIC,
    LATE_TOPIC,
    RAW_TOPIC,
    SESSIONS_TOPIC,
    SUBJECT,
    VALID_TOPIC,
    WINDOWS_TOPIC,
    Settings,
    load_contract,
)

log = logging.getLogger("bootstrap")

# topic -> partitions. Six on the hot path so two Flink subtasks get three each.
TOPICS = {
    RAW_TOPIC: 6,
    VALID_TOPIC: 6,
    CLEAN_TOPIC: 6,
    DLQ_TOPIC: 1,
    LATE_TOPIC: 2,
    WINDOWS_TOPIC: 2,
    SESSIONS_TOPIC: 2,
}
COMPATIBILITY = "FULL_TRANSITIVE"


def create_topics(settings: Settings) -> None:
    rf = int(os.environ.get("TOPIC_REPLICATION_FACTOR", "3"))
    min_isr = os.environ.get("TOPIC_MIN_INSYNC_REPLICAS", "2")
    admin = AdminClient({"bootstrap.servers": settings.bootstrap})
    for attempt in range(60):
        try:
            existing = set(admin.list_topics(timeout=5).topics)
            break
        except KafkaException as e:
            log.info("waiting for Kafka (%s): %s", attempt, e)
            time.sleep(2)
    else:
        raise SystemExit("Kafka did not come up")
    new = [
        NewTopic(
            t,
            num_partitions=n,
            replication_factor=rf,
            config={
                "min.insync.replicas": min_isr,
                "retention.ms": str(24 * 3600 * 1000),
                "unclean.leader.election.enable": "false",
            },
        )
        for t, n in TOPICS.items()
        if t not in existing
    ]
    for topic, fut in (admin.create_topics(new) if new else {}).items():
        fut.result()
        log.info("created %s", topic)


def register_contract(settings: Settings) -> int:
    reg = Registry(settings.registry_url)
    for attempt in range(60):
        try:
            reg.http.get("/subjects").raise_for_status()
            break
        except Exception as e:
            log.info("waiting for Schema Registry (%s): %s", attempt, e)
            time.sleep(2)
    try:
        current = reg.compatibility(SUBJECT)
    except RegistryError:
        current = None
    if current != COMPATIBILITY:
        reg.set_compatibility(SUBJECT, COMPATIBILITY)
    schema_id = reg.register(SUBJECT, load_contract("v1.avsc"))
    log.info("%s: compatibility %s, v1 is schema id %d", SUBJECT, COMPATIBILITY, schema_id)
    return schema_id


def main(settings: Settings) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    create_topics(settings)
    register_contract(settings)
