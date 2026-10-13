"""Count every event of a run at every layer, against the generator's ledger.

Exactly-once is a claim about counts, so it is checked as counts: for one run id,
the set of event ids at each layer must equal the set the ledger says should be
there — no fewer (loss), no more (duplicates) — and the dead-letter topic must hold
exactly the poison the generator injected.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import clickhouse_connect
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from .settings import CLEAN_TOPIC, DLQ_TOPIC, LATE_TOPIC, VALID_TOPIC, Settings

ICEBERG_TABLE = "clickstream.events"


@dataclass
class Layer:
    rows: int  # every row or message, duplicates included
    unique: int  # distinct event ids
    missing: int  # expected ids not present
    unexpected: int  # ids present that should not be (poison that leaked)

    @property
    def exact(self) -> bool:
        return self.rows == self.unique and self.missing == 0 and self.unexpected == 0


def expected_ids(ledger: dict[str, Any]) -> set[str]:
    poison = {s for seqs in ledger["poison"].values() for s in seqs}
    run = ledger["run_id"]
    return {f"{run}-{s:09d}" for s in range(ledger["slots"]) if s not in poison}


def compare(ids: Iterable[str], expected: set[str]) -> Layer:
    counts = Counter(ids)
    present = set(counts)
    return Layer(
        rows=sum(counts.values()),
        unique=len(present),
        missing=len(expected - present),
        unexpected=len(present - expected),
    )


def scan_topic(
    settings: Settings, topic: str, prefix: str, headers: bool = False
) -> list[tuple[str, dict[str, str]]]:
    """Every committed message in ``topic`` whose key starts with ``prefix``."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap,
            "group.id": f"reconcile-{prefix}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    meta = consumer.list_topics(topic, timeout=10).topics[topic]
    ends: dict[int, int] = {}
    parts = []
    for p in meta.partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        if high > 0:
            ends[p] = high
            parts.append(TopicPartition(topic, p, OFFSET_BEGINNING))
    consumer.assign(parts)
    out: list[tuple[str, dict[str, str]]] = []
    done: set[int] = set()
    idle = 0
    while len(done) < len(ends) and idle < 20:
        batch = consumer.consume(num_messages=10_000, timeout=1.0)
        if not batch:
            idle += 1
            continue
        idle = 0
        for m in batch:
            part, offset = m.partition(), m.offset()
            if m.error() or part is None or offset is None:
                continue
            if offset >= ends[part] - 1:
                done.add(part)
            raw_key = m.key()
            key = raw_key.decode(errors="replace") if isinstance(raw_key, bytes) else str(raw_key)
            if key.startswith(prefix):
                hdrs = _headers(m.headers()) if headers else {}
                out.append((key, hdrs))
        # A transactional topic ends in a commit marker the consumer never sees, so a
        # partition is also done when its position reaches the end offset.
        for tp in consumer.position([TopicPartition(topic, p) for p in ends]):
            if tp.offset >= ends[tp.partition]:
                done.add(tp.partition)
    consumer.close()
    return out


def _headers(raw: Any) -> dict[str, str]:
    items = raw.items() if isinstance(raw, dict) else (raw or [])
    return {str(k): _text(v) for k, v in items}


def _text(value: str | bytes | None) -> str:
    return value.decode(errors="replace") if isinstance(value, bytes) else str(value)


def clickhouse(settings: Settings) -> Any:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )


def iceberg_ids(settings: Settings, prefix: str) -> list[str]:
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        "lake",
        type="rest",
        uri=settings.iceberg_uri,
        **{
            "s3.endpoint": settings.s3_endpoint,
            "s3.access-key-id": settings.s3_access_key,
            "s3.secret-access-key": settings.s3_secret_key,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )
    table = catalog.load_table(ICEBERG_TABLE)
    arrow = table.scan(row_filter=f"event_id LIKE '{prefix}%'", selected_fields=("event_id",))
    return [str(v) for v in arrow.to_arrow().column("event_id").to_pylist()]


def reconcile(settings: Settings, ledger_path: Path, layers: list[str]) -> dict[str, Any]:
    ledger = json.loads(ledger_path.read_text())
    run = ledger["run_id"]
    prefix = f"{run}-"
    expected = expected_ids(ledger)
    late = {f"{run}-{s:09d}" for s in ledger["late"]}
    poison_total = sum(len(v) for v in ledger["poison"].values())
    report: dict[str, Any] = {
        "run_id": run,
        "ledger": {
            "slots": ledger["slots"],
            "expected_valid": len(expected),
            "poison": {k: len(v) for k, v in ledger["poison"].items()},
            "poison_total": poison_total,
            "late": len(ledger["late"]),
            "out_of_order": len(ledger["out_of_order"]),
            "duplicated": len(ledger["duplicated"]),
            "producer_failed": ledger["failed"],
        },
        "layers": {},
    }
    out = report["layers"]

    if "kafka" in layers:
        valid = [k for k, _ in scan_topic(settings, VALID_TOPIC, prefix)]
        # The gatekeeper does not deduplicate: events.valid must hold every send.
        v = compare(valid, expected)
        out["events.valid"] = {
            **v.__dict__,
            "duplicates_expected": len(ledger["duplicated"]),
            "exact": v.unique == len(expected)
            and v.missing == 0
            and v.unexpected == 0
            and v.rows == len(expected) + len(ledger["duplicated"]),
        }
        clean = compare([k for k, _ in scan_topic(settings, CLEAN_TOPIC, prefix)], expected)
        out["events.clean"] = {**clean.__dict__, "exact": clean.exact}
        dlq = scan_topic(settings, DLQ_TOPIC, run, headers=True)
        reasons = Counter(h.get("error.reason", "?") for _, h in dlq)
        out["events.dlq"] = {
            "rows": len(dlq),
            "injected": poison_total,
            "exact": len(dlq) == poison_total,
            "reasons": dict(reasons),
        }
        late_seen = [k for k, _ in scan_topic(settings, LATE_TOPIC, prefix)]
        out["events.late"] = {
            "rows": len(late_seen),
            "unique": len(set(late_seen)),
            "sent_late": len(late),
            "matches_ledger": set(late_seen) == late and len(late_seen) == len(late),
        }

    if "clickhouse" in layers:
        ch = clickhouse(settings)
        q = "SELECT event_id FROM lumen.events {final} WHERE startsWith(event_id, {{p:String}})"
        raw = [r[0] for r in ch.query(q.format(final=""), parameters={"p": prefix}).result_rows]
        fin = [
            r[0] for r in ch.query(q.format(final="FINAL"), parameters={"p": prefix}).result_rows
        ]
        layer = compare(fin, expected)
        out["clickhouse.events"] = {
            **layer.__dict__,
            "rows_before_merge": len(raw),
            "exact": layer.exact,
        }
        lo, hi = ledger["started_at_ms"], ledger["ended_at_ms"] + 15_000
        windowed = ch.query(
            "SELECT sum(events) FROM lumen.window_counts FINAL "
            "WHERE window_start >= fromUnixTimestamp64Milli({lo:Int64}) - INTERVAL 10 SECOND "
            "AND window_start < fromUnixTimestamp64Milli({hi:Int64})",
            parameters={"lo": lo, "hi": hi},
        ).result_rows[0][0]
        late_rows = ch.query(
            "SELECT count(), uniqExact(event_id) FROM lumen.late_events FINAL "
            "WHERE startsWith(event_id, {p:String})",
            parameters={"p": prefix},
        ).result_rows[0]
        on_time = len(expected) - len(late)
        out["windows_vs_late"] = {
            "on_time_expected": on_time,
            "counted_in_windows": int(windowed or 0),
            "late_expected": len(late),
            "late_side_output": int(late_rows[1]),
            "exact": int(windowed or 0) == on_time and int(late_rows[1]) == len(late),
        }
        dlq_rows = ch.query(
            "SELECT count() FROM lumen.dlq FINAL WHERE startsWith(key, {p:String})",
            parameters={"p": run},
        ).result_rows[0][0]
        out["clickhouse.dlq"] = {"rows": int(dlq_rows), "exact": int(dlq_rows) == poison_total}

    if "iceberg" in layers:
        ice = compare(iceberg_ids(settings, prefix), expected)
        out["iceberg.events"] = {**ice.__dict__, "exact": ice.exact}

    report["exact"] = all(bool(v.get("exact", v.get("matches_ledger", True))) for v in out.values())
    return report


def latency(settings: Settings, ledger_path: Path) -> dict[str, Any]:
    """Event time to queryable in ClickHouse, for events sent on time and in order."""
    import numpy as np

    ledger = json.loads(ledger_path.read_text())
    run = ledger["run_id"]
    skip = {f"{run}-{s:09d}" for s in ledger["late"] + ledger["out_of_order"]}
    rows = (
        clickhouse(settings)
        .query(
            "SELECT event_id, toUnixTimestamp64Milli(min(_ingested_at)) - "
            "toUnixTimestamp64Milli(any(event_time)) FROM lumen.events "
            "WHERE startsWith(event_id, {p:String}) GROUP BY event_id",
            parameters={"p": f"{run}-"},
        )
        .result_rows
    )
    lat = np.array([ms for eid, ms in rows if eid not in skip], dtype=np.float64) / 1000
    if lat.size == 0:
        return {"run_id": run, "n": 0}
    p50, p95, p99 = np.percentile(lat, [50, 95, 99])
    duration_s = (ledger["ended_at_ms"] - ledger["started_at_ms"]) / 1000
    return {
        "run_id": run,
        "n": int(lat.size),
        "p50_s": round(float(p50), 3),
        "p95_s": round(float(p95), 3),
        "p99_s": round(float(p99), 3),
        "max_s": round(float(lat.max()), 3),
        "offered_rate": ledger["rate"],
        "achieved_send_rate": round(ledger["slots"] / ledger["duration_s"], 1),
        "sends": ledger["sends"],
        "duration_s": round(duration_s, 1),
    }
