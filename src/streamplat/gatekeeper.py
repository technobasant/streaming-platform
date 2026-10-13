"""The contract gate between producers and the stream processor.

Reads ``clickstream.raw`` (Confluent-framed Avro), and in one Kafka transaction per
batch writes each record either to ``events.valid`` as JSON keyed by ``event_id``, or
to ``events.dlq`` byte-for-byte with the reason in a header — and commits the input
offsets in the same transaction. A crash anywhere replays the batch; nothing is
written twice and nothing is skipped.

Flink's ``avro-confluent`` format can fail a job on a bad record or drop it silently,
but it cannot route it anywhere. That is why this stage exists.
"""

from __future__ import annotations

import json
import logging
import signal
import time
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from prometheus_client import Counter, Histogram, start_http_server

from . import wire
from .registry import Decoder, Registry, RegistryError
from .settings import DLQ_TOPIC, RAW_TOPIC, VALID_TOPIC, Settings, load_contract
from .validation import violations

log = logging.getLogger("gatekeeper")

RECORDS = Counter("sp_gatekeeper_records", "Records routed", ["outcome"])
DLQ = Counter("sp_gatekeeper_dlq", "Records sent to the dead-letter topic", ["reason"])
TXN = Histogram(
    "sp_gatekeeper_transaction_seconds",
    "Begin to commit, per batch",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
ABORTS = Counter("sp_gatekeeper_aborted_transactions", "Transactions aborted and replayed")


def to_json(record: dict[str, Any]) -> bytes:
    """The JSON shape Flink and ClickHouse read: lower-case enums, UTC 'YYYY-MM-DD HH:MM:SS.mmm'."""
    ts = record["event_time"]
    if not isinstance(ts, datetime):
        ts = datetime.fromtimestamp(int(ts) / 1000, tz=UTC)
    out = {
        "event_id": record["event_id"],
        "event_type": str(record["event_type"]).lower(),
        "event_time": ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "session_id": record["session_id"],
        "customer_id": record.get("customer_id"),
        "client": str(record["client"]).lower(),
        "page": record.get("page"),
        "product_id": record.get("product_id"),
        "quantity": record.get("quantity"),
        "order_id": record.get("order_id"),
        "amount_cents": record.get("amount_cents"),
        "currency": record.get("currency", "USD"),
        "is_probe": bool(record.get("is_probe", False)),
    }
    return json.dumps(out, separators=(",", ":")).encode()


def classify(decoder: Decoder, raw: bytes | None) -> tuple[dict[str, Any] | None, str | None]:
    """(record, None) for a valid record, (None, reason) for poison."""
    if raw is None:
        return None, "tombstone"
    if wire.parse_header(raw) is None:
        return None, "not_confluent_avro"
    try:
        record = decoder.decode(raw)
    except RegistryError as e:
        return None, "schema_not_found" if e.status == 404 else f"registry_{e.status}"
    except Exception:  # fastavro raises EOFError, ValueError, IndexError... on bad bytes
        return None, "avro_decode_error"
    broken = violations(record)
    if broken:
        return None, f"rule:{broken[0]}"
    return record, None


def _dlq_headers(msg: Message, reason: str) -> list[tuple[str, str | bytes | None]]:
    return [
        ("error.reason", reason.encode()),
        ("source.topic", (msg.topic() or "").encode()),
        ("source.partition", str(msg.partition()).encode()),
        ("source.offset", str(msg.offset()).encode()),
    ]


class Gatekeeper:
    def __init__(self, settings: Settings, batch: int = 2000, linger_s: float = 0.05) -> None:
        self.batch = batch
        self.linger_s = linger_s
        self.decoder = Decoder(Registry(settings.registry_url), load_contract("v1.avsc"))
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap,
                "group.id": "gatekeeper",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
                "partition.assignment.strategy": "cooperative-sticky",
                "socket.connection.setup.timeout.ms": 5000,
            }
        )
        self.producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap,
                "transactional.id": "gatekeeper-0",
                "enable.idempotence": True,
                "linger.ms": 5,
                "compression.type": "lz4",
                "transaction.timeout.ms": 60_000,
                # The default is 30 s. When the transaction coordinator's broker died,
                # the producer spent that long reconnecting to it before it looked
                # for the new coordinator, and the whole pipeline waited.
                "socket.connection.setup.timeout.ms": 5000,
            }
        )
        self.running = True

    def stop(self, *_: object) -> None:
        self.running = False

    def _rewind(self) -> None:
        """After an abort, go back to the last committed offsets and read the batch again."""
        assigned = self.consumer.assignment()
        for tp in self.consumer.committed(assigned, timeout=10):
            offset = tp.offset if tp.offset >= 0 else 0
            self.consumer.seek(TopicPartition(tp.topic, tp.partition, offset))

    def process(self, msgs: list[Message]) -> None:
        self.producer.begin_transaction()
        started = time.perf_counter()
        valid = dlq = 0
        for msg in msgs:
            error = msg.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(error)
            record, reason = classify(self.decoder, msg.value())
            if record is not None:
                self.producer.produce(VALID_TOPIC, key=record["event_id"], value=to_json(record))
                valid += 1
            else:
                assert reason is not None
                self.producer.produce(
                    DLQ_TOPIC, key=msg.key(), value=msg.value(), headers=_dlq_headers(msg, reason)
                )
                DLQ.labels(reason).inc()
                dlq += 1
        positions = self.consumer.position(self.consumer.assignment())
        self.producer.send_offsets_to_transaction(
            positions, self.consumer.consumer_group_metadata()
        )
        while True:
            try:
                self.producer.commit_transaction()
                break
            except KafkaException as e:
                if not e.args[0].retriable():
                    raise
                log.warning("retrying commit: %s", e.args[0])  # e.g. a broker just died
        TXN.observe(time.perf_counter() - started)
        RECORDS.labels("valid").inc(valid)
        RECORDS.labels("dlq").inc(dlq)

    def run(self) -> None:
        self.producer.init_transactions()
        self.consumer.subscribe([RAW_TOPIC])
        log.info("gatekeeper running: %s -> %s | %s", RAW_TOPIC, VALID_TOPIC, DLQ_TOPIC)
        while self.running:
            msgs = self.consumer.consume(num_messages=self.batch, timeout=self.linger_s)
            if not msgs:
                continue
            try:
                self.process(msgs)
            except KafkaException as e:
                err = e.args[0]
                if isinstance(err, KafkaError) and err.txn_requires_abort():
                    log.warning("aborting transaction: %s", err)
                    # If the abort cannot complete either, exit: a restarted process
                    # fences this one (init_transactions) and resumes from the last
                    # committed offsets, which is just as exactly-once.
                    self.producer.abort_transaction()
                    ABORTS.inc()
                    self._rewind()
                    continue
                raise  # fatal (e.g. fenced): exit and let the container restart
        self.consumer.close()


def main(settings: Settings, metrics_port: int = 9109) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    start_http_server(metrics_port)
    gk = Gatekeeper(settings)
    signal.signal(signal.SIGTERM, gk.stop)
    signal.signal(signal.SIGINT, gk.stop)
    gk.run()
