"""The freshness SLI, measured from outside the pipeline.

Every ``interval`` seconds a probe event — valid against the contract, flagged
``is_probe`` — goes into ``clickstream.raw`` exactly like a shopper's event. The
exporter then watches ClickHouse for it. A probe is *good* when it is queryable within
``threshold`` seconds of its event time, *bad* when it is late or never shows up.

A black-box probe measures what users of the table experience, and it keeps measuring
when no real traffic flows; the alternative, comparing ``now()`` with the newest event
in the table, pages on every quiet period.

The exporter also reports consumer lag per group and under-replicated partitions.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any

import clickhouse_connect
from confluent_kafka import (
    Consumer,
    ConsumerGroupTopicPartitions,
    Producer,
    TopicCollection,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .registry import Encoder, Registry
from .settings import (
    CLEAN_TOPIC,
    RAW_TOPIC,
    SESSIONS_TOPIC,
    SUBJECT,
    VALID_TOPIC,
    WINDOWS_TOPIC,
    Settings,
    load_contract,
)

log = logging.getLogger("exporter")

PROBES = Counter("sp_freshness_probes", "Probes resolved (landed or past the threshold)")
GOOD = Counter("sp_freshness_probes_good", "Probes queryable within the threshold")
SENT = Counter("sp_freshness_probes_sent", "Probes produced")
LATENCY = Histogram(
    "sp_freshness_probe_latency_seconds",
    "Probe event time to queryable in ClickHouse",
    buckets=(1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 30, 60, 120, 300),
)
OLDEST = Gauge("sp_freshness_oldest_unlanded_probe_seconds", "Age of the oldest probe not yet seen")
THRESHOLD = Gauge("sp_freshness_threshold_seconds", "A probe slower than this is bad")
LAG = Gauge("sp_consumer_lag", "End offset minus committed offset", ["group", "topic"])
URP = Gauge(
    "sp_under_replicated_partitions", "Partitions with fewer in-sync replicas than replicas"
)
OFFLINE = Gauge("sp_offline_partitions", "Partitions without a leader")

GROUPS = {
    "gatekeeper": RAW_TOPIC,
    "flink-clean": VALID_TOPIC,
    "flink-windows": CLEAN_TOPIC,
    "clickhouse-events": CLEAN_TOPIC,
}
WATCHED_TOPICS = [RAW_TOPIC, VALID_TOPIC, CLEAN_TOPIC, WINDOWS_TOPIC, SESSIONS_TOPIC]


@dataclass
class Probe:
    event_id: str
    sent_ms: int
    resolved: bool = False


def probe_record(event_id: str, now_ms: int) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event_type": "PAGE_VIEW",
        "event_time": now_ms,
        "session_id": "probe",
        "customer_id": None,
        "client": "API",
        "page": "/healthz",
        "product_id": None,
        "quantity": None,
        "order_id": None,
        "amount_cents": None,
        "currency": "USD",
        "is_probe": True,
    }


class Exporter:
    def __init__(self, settings: Settings, interval_s: float, threshold_s: float) -> None:
        self.interval_s = interval_s
        self.threshold_ms = int(threshold_s * 1000)
        THRESHOLD.set(threshold_s)
        self.settings = settings
        self.encoder = Encoder(Registry(settings.registry_url), SUBJECT, load_contract())
        self.producer = Producer(
            {"bootstrap.servers": settings.bootstrap, "enable.idempotence": True, "linger.ms": 0}
        )
        self.admin = AdminClient({"bootstrap.servers": settings.bootstrap})
        self.offsets = Consumer(
            {"bootstrap.servers": settings.bootstrap, "group.id": "exporter-offsets"}
        )
        self.ch = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )
        self.outstanding: dict[str, Probe] = {}

    def send_probe(self) -> None:
        now_ms = int(time.time() * 1000)
        event_id = f"probe-{uuid.uuid4().hex[:16]}"
        self.producer.produce(
            RAW_TOPIC, key="probe", value=self.encoder.encode(probe_record(event_id, now_ms))
        )
        self.producer.poll(0)
        self.outstanding[event_id] = Probe(event_id, now_ms)
        SENT.inc()

    def check_probes(self) -> None:
        now_ms = int(time.time() * 1000)
        if self.outstanding:
            rows = self.ch.query(
                "SELECT event_id, toUnixTimestamp64Milli(min(_ingested_at)) FROM lumen.events "
                "WHERE event_id IN {ids:Array(String)} GROUP BY event_id",
                parameters={"ids": list(self.outstanding)},
            ).result_rows
            for event_id, ingested_ms in rows:
                p = self.outstanding.pop(event_id)
                latency = int(ingested_ms) - p.sent_ms
                LATENCY.observe(latency / 1000)
                if not p.resolved:
                    PROBES.inc()
                    if latency <= self.threshold_ms:
                        GOOD.inc()
        for p in list(self.outstanding.values()):
            age = now_ms - p.sent_ms
            if not p.resolved and age > self.threshold_ms:
                p.resolved = True  # counted bad now; still watched so its latency is recorded
                PROBES.inc()
            if age > 3_600_000:
                del self.outstanding[p.event_id]
        oldest = min((p.sent_ms for p in self.outstanding.values()), default=now_ms)
        OLDEST.set((now_ms - oldest) / 1000)

    def check_kafka(self) -> None:
        for group, topic in GROUPS.items():
            try:
                meta = self.offsets.list_topics(topic, timeout=5).topics[topic]
                parts = [TopicPartition(topic, p) for p in meta.partitions]
                req = self.admin.list_consumer_group_offsets(
                    [ConsumerGroupTopicPartitions(group, parts)], request_timeout=5
                )
                committed = req[group].result().topic_partitions
                lag = 0
                for tp in committed:
                    _, high = self.offsets.get_watermark_offsets(
                        TopicPartition(topic, tp.partition), timeout=5
                    )
                    lag += high - tp.offset if tp.offset >= 0 else high
                LAG.labels(group, topic).set(lag)
            except Exception as e:  # a broker being down must not stop the probes
                log.debug("lag for %s: %s", group, e)
        try:
            futures = self.admin.describe_topics(TopicCollection(WATCHED_TOPICS), request_timeout=5)
            urp = offline = 0
            for fut in futures.values():
                for p in fut.result().partitions:
                    urp += len(p.isr) < len(p.replicas)
                    offline += p.leader is None
            URP.set(urp)
            OFFLINE.set(offline)
        except Exception as e:
            log.debug("describe_topics: %s", e)

    def run(self) -> None:
        next_probe = time.monotonic()
        next_kafka = time.monotonic()
        while True:
            now = time.monotonic()
            if now >= next_probe:
                self.send_probe()
                next_probe += self.interval_s
            try:
                self.check_probes()
            except Exception as e:
                log.warning("probe check failed: %s", e)
            if now >= next_kafka:
                self.check_kafka()
                next_kafka = now + 5
            time.sleep(0.5)


def main(settings: Settings, interval_s: float, threshold_s: float, port: int = 9108) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    start_http_server(port)
    Exporter(settings, interval_s, threshold_s).run()
