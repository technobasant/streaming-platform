"""Send the seeded storefront to Kafka at a fixed rate, and write a ledger of what was sent.

The ledger is the ground truth the reconciler checks every downstream layer against:
which sequence numbers were poison, which were sent late, out of order or twice.
"""

from __future__ import annotations

import heapq
import json
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from . import wire
from .events import Emission, Faults, schedule
from .registry import Encoder, Registry
from .settings import RAW_TOPIC, SUBJECT, Settings, load_contract

UNKNOWN_SCHEMA_ID = 999_999


@dataclass
class Ledger:
    run_id: str
    seed: int
    rate: float
    duration_s: float
    faults: dict[str, Any]
    slots: int = 0
    sends: int = 0
    poison: dict[str, list[int]] = field(default_factory=dict)
    late: list[int] = field(default_factory=list)
    out_of_order: list[int] = field(default_factory=list)
    duplicated: list[int] = field(default_factory=list)
    started_at_ms: int = 0
    ended_at_ms: int = 0
    acked: int = 0
    failed: int = 0
    errors: dict[str, int] = field(default_factory=dict)
    # second since start -> [acked, failed, max ack latency ms, sum ack latency ms]
    timeline: dict[int, list[float]] = field(default_factory=dict)

    @property
    def poison_seqs(self) -> set[int]:
        return {s for seqs in self.poison.values() for s in seqs}

    @property
    def expected_valid(self) -> int:
        return self.slots - len(self.poison_seqs)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        body = asdict(self)
        body["expected_valid"] = self.expected_valid
        body["poison_total"] = len(self.poison_seqs)
        path.write_text(json.dumps(body, indent=1))


def load_ledger(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text())
    return data


def producer_config(settings: Settings) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap,
        "client.id": "generator",
        "enable.idempotence": True,  # implies acks=all and bounded in-flight requests
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "lz4",
        "delivery.timeout.ms": 120_000,
        "queue.buffering.max.messages": 500_000,
    }


def run(
    settings: Settings,
    run_id: str,
    rate: float,
    duration_s: float,
    seed: int,
    faults: Faults,
    ledger_path: Path,
    schema_file: str = "v1.avsc",
) -> Ledger:
    registry = Registry(settings.registry_url)
    # Fails here, before anything is sent, if the schema is not registered for the subject.
    encoder = Encoder(registry, SUBJECT, load_contract(schema_file))
    producer = Producer(producer_config(settings))
    ledger = Ledger(run_id, seed, rate, duration_s, asdict(faults))
    t0 = time.time()
    ledger.started_at_ms = int(t0 * 1000)

    def on_delivery(err: KafkaError | None, msg: Message) -> None:
        sec = int(time.time() - t0)
        bucket = ledger.timeline.setdefault(sec, [0, 0, 0.0, 0.0])
        if err is not None:
            ledger.failed += 1
            ledger.errors[err.name()] = ledger.errors.get(err.name(), 0) + 1
            bucket[1] += 1
            return
        ledger.acked += 1
        lat_ms = (msg.latency() or 0.0) * 1000
        bucket[0] += 1
        bucket[2] = max(bucket[2], lat_ms)
        bucket[3] += lat_ms

    def send(key: str, value: bytes) -> None:
        while True:
            try:
                producer.produce(RAW_TOPIC, key=key, value=value, on_delivery=on_delivery)
                ledger.sends += 1
                return
            except BufferError:
                producer.poll(0.05)  # local queue full: back-pressure, not loss

    held: list[tuple[float, int, str, bytes]] = []  # (release at, tiebreak, key, value)
    plan = schedule(run_id, seed, faults)
    end = t0 + duration_s
    while True:
        now = time.time()
        if now >= end:
            break
        due = int((now - t0) * rate)
        while ledger.slots < due:
            em = next(plan)
            ledger.slots += 1
            _emit(em, encoder, now, ledger, held, send)
        while held and held[0][0] <= now:
            _, _, key, value = heapq.heappop(held)
            send(key, value)
        producer.poll(0)
        time.sleep(0.002)
    for _, _, key, value in sorted(held):
        send(key, value)
    remaining = producer.flush(120)
    ledger.ended_at_ms = int(time.time() * 1000)
    if remaining:
        ledger.errors["undelivered_at_exit"] = remaining
    ledger.save(ledger_path)
    return ledger


def _emit(
    em: Emission,
    encoder: Encoder,
    now: float,
    ledger: Ledger,
    held: list[tuple[float, int, str, bytes]],
    send: Callable[[str, bytes], None],
) -> None:
    now_ms = int(now * 1000)
    if em.poison is not None:
        ledger.poison.setdefault(em.poison, []).append(em.seq)
        if em.poison == "not_avro":
            value = em.junk
        elif em.poison == "unknown_schema":
            value = wire.frame(UNKNOWN_SCHEMA_ID, em.junk)
        else:
            assert em.record is not None
            em.record["event_time"] = now_ms
            value = encoder.encode(em.record)
            if em.poison == "truncated":
                value = value[: max(wire.HEADER.size + 1, len(value) // 2)]
        send(em.key, value)
        return

    assert em.record is not None
    em.record["event_time"] = now_ms - em.late_by_ms
    value = encoder.encode(em.record)
    if em.late_by_ms:
        ledger.late.append(em.seq)
    if em.delay_ms:
        ledger.out_of_order.append(em.seq)
        heapq.heappush(held, (now + em.delay_ms / 1000, em.seq, em.key, value))
    else:
        send(em.key, value)
    if em.duplicate_after_ms is not None:
        ledger.duplicated.append(em.seq)
        release = now + (em.delay_ms + em.duplicate_after_ms) / 1000
        heapq.heappush(held, (release, -em.seq - 1, em.key, value))
