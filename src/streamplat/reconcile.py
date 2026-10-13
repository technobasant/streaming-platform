"""Count every event of a run at every layer, against the generator's ledger.

Exactly-once is a claim about counts, so it is checked as counts: for one run id,
the set of event ids at each layer must equal the set the ledger says should be
there — no fewer (loss), no more (duplicates) — and the dead-letter topic must hold
exactly the poison the generator injected.
"""

from __future__ import annotations

import json
from array import array
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import clickhouse_connect
import numpy as np
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from numpy.typing import NDArray

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


def poison_seqs(ledger: dict[str, Any]) -> list[int]:
    return sorted({s for seqs in ledger["poison"].values() for s in seqs})


def expected_ids(ledger: dict[str, Any]) -> set[str]:
    poison = set(poison_seqs(ledger))
    run = ledger["run_id"]
    return {f"{run}-{s:09d}" for s in range(ledger["slots"]) if s not in poison}


def tally(seqs: NDArray[np.int64], ledger: dict[str, Any]) -> Layer:
    """Compare the sequence numbers found at a layer with the ledger's expectation.

    Event ids are ``<run>-<seq>``, so a run's ids are the integers 0..slots-1 minus the
    poison: counting with one array per layer keeps half a million events in a few MB.
    """
    slots = int(ledger["slots"])
    inside = seqs[(seqs >= 0) & (seqs < slots)]
    outside = seqs[(seqs < 0) | (seqs >= slots)]
    counts = np.bincount(inside, minlength=slots)
    expected = np.ones(slots, dtype=bool)
    expected[poison_seqs(ledger)] = False
    outside_unique = int(np.unique(outside).size)
    return Layer(
        rows=int(seqs.size),
        unique=int((counts > 0).sum()) + outside_unique,
        missing=int((counts[expected] == 0).sum()),
        unexpected=int((counts[~expected] > 0).sum()) + outside_unique,
    )


def compare(ids: list[str], expected: set[str]) -> Layer:
    """The same comparison on explicit ids (used by the tests)."""
    present = set(ids)
    return Layer(
        rows=len(ids),
        unique=len(present),
        missing=len(expected - present),
        unexpected=len(present - expected),
    )


def _seq(key: str, prefix: str) -> int:
    try:
        return int(key[len(prefix) :])
    except ValueError:
        return -1


def scan_topic(
    settings: Settings,
    topic: str,
    prefix: str,
    isolation: str = "read_committed",
    since_ms: int = 0,
    reasons: bool = False,
) -> tuple[NDArray[np.int64], Counter[str]]:
    """Sequence numbers of every message in ``topic`` whose key starts with ``prefix``,
    and with ``reasons`` a count of their ``error.reason`` headers.

    With ``read_uncommitted`` the scan also sees records from aborted transactions:
    the difference between the two counts is what a restart rolled back and redid.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap,
            "group.id": f"reconcile-{prefix}",
            "enable.auto.commit": False,
            "isolation.level": isolation,
            # The default pre-fetch queue is 64 MB per consumer; a scan does not need it.
            "queued.max.messages.kbytes": 8192,
        }
    )
    meta = consumer.list_topics(topic, timeout=10).topics[topic]
    ends: dict[int, int] = {}
    parts = []
    # Start each partition at the first message written at or after since_ms, so a
    # run is reconciled without rereading every run before it.
    starts = {
        tp.partition: tp.offset
        for tp in consumer.offsets_for_times(
            [TopicPartition(topic, p, since_ms) for p in meta.partitions], timeout=10
        )
    }
    for p in meta.partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        start = starts.get(p, -1) if since_ms else OFFSET_BEGINNING
        if high > 0 and (start == OFFSET_BEGINNING or 0 <= start < high):
            ends[p] = high
            parts.append(TopicPartition(topic, p, start))
    consumer.assign(parts)
    seqs = array("q")
    why: Counter[str] = Counter()
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
                seqs.append(_seq(key, prefix))
                if reasons:
                    why[_headers(m.headers()).get("error.reason", "?")] += 1
        # A transactional topic ends in a commit marker the consumer never sees, so a
        # partition is also done when its position reaches the end offset.
        for tp in consumer.position([TopicPartition(topic, p) for p in ends]):
            if tp.offset >= ends[tp.partition]:
                done.add(tp.partition)
    consumer.close()
    return np.frombuffer(seqs, dtype=np.int64), why


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


def ch_seqs(ch: Any, table: str, prefix: str, final: bool) -> NDArray[np.int64]:
    """Sequence numbers of a run's rows in a ClickHouse table, one per row."""
    sql = (
        f"SELECT toInt64OrDefault(substring(event_id, {len(prefix) + 1}), toInt64(-1)) "
        f"FROM {table} {'FINAL' if final else ''} WHERE startsWith(event_id, {{p:String}})"
    )
    result = ch.query_np(sql, parameters={"p": prefix})
    return np.asarray(result[:, 0] if result.size else result, dtype=np.int64).reshape(-1)


def iceberg_seqs(settings: Settings, prefix: str, since_ms: int) -> NDArray[np.int64]:
    import pyarrow.compute as pc
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
    # The event-time bound lets Iceberg skip every data file written before the run
    # (file-level min/max statistics); without it each reconcile read the whole table.
    since = datetime.fromtimestamp(since_ms / 1000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    scan = table.scan(
        row_filter=f"event_id LIKE '{prefix}%' AND event_time >= '{since}'",
        selected_fields=("event_id",),
    )
    ids = scan.to_arrow().column("event_id")
    seqs = pc.cast(pc.utf8_slice_codeunits(ids, len(prefix)), "int64")
    return np.asarray(seqs.to_numpy(zero_copy_only=False), dtype=np.int64)


GROUP_OF = {
    "events.valid": "kafka",
    "events.clean": "kafka",
    "events.dlq": "kafka",
    "events.late": "kafka",
    "clickhouse.events": "clickhouse",
    "windows_vs_late": "clickhouse",
    "clickhouse.dlq": "clickhouse",
    "iceberg.events": "iceberg",
}


def failing_groups(report: dict[str, Any]) -> list[str]:
    """The layer groups to read again: those with any layer not yet exact."""
    return sorted(
        {
            GROUP_OF[name]
            for name, layer in report["layers"].items()
            if not layer.get("exact", layer.get("matches_ledger", True))
        }
    )


def reconcile(settings: Settings, ledger_path: Path, layers: list[str]) -> dict[str, Any]:
    ledger = json.loads(ledger_path.read_text())
    run = ledger["run_id"]
    prefix = f"{run}-"
    n_poison = len(poison_seqs(ledger))
    n_expected = int(ledger["slots"]) - n_poison
    late = np.array(sorted(ledger["late"]), dtype=np.int64)
    report: dict[str, Any] = {
        "run_id": run,
        "ledger": {
            "slots": ledger["slots"],
            "expected_valid": n_expected,
            "poison": {k: len(v) for k, v in ledger["poison"].items()},
            "poison_total": n_poison,
            "late": len(ledger["late"]),
            "out_of_order": len(ledger["out_of_order"]),
            "duplicated": len(ledger["duplicated"]),
            "producer_failed": ledger["failed"],
        },
        "layers": {},
    }
    out = report["layers"]
    # Kafka timestamps are write times; a minute's margin covers clock skew and lag.
    since = int(ledger["started_at_ms"]) - 60_000

    if "kafka" in layers:
        valid, _ = scan_topic(settings, VALID_TOPIC, prefix, since_ms=since)
        # The gatekeeper does not deduplicate: events.valid must hold every send.
        v = tally(valid, ledger)
        uncommitted, _ = scan_topic(
            settings, VALID_TOPIC, prefix, isolation="read_uncommitted", since_ms=since
        )
        out["events.valid"] = {
            **v.__dict__,
            "duplicates_expected": len(ledger["duplicated"]),
            "aborted_and_rewritten": int(uncommitted.size - valid.size),
            "exact": v.unique == n_expected
            and v.missing == 0
            and v.unexpected == 0
            and v.rows == n_expected + len(ledger["duplicated"]),
        }
        del valid, uncommitted
        clean_seqs, _ = scan_topic(settings, CLEAN_TOPIC, prefix, since_ms=since)
        clean = tally(clean_seqs, ledger)
        everything, _ = scan_topic(
            settings, CLEAN_TOPIC, prefix, isolation="read_uncommitted", since_ms=since
        )
        out["events.clean"] = {
            **clean.__dict__,
            # Written inside a transaction that was then aborted (a restart), and
            # written again after it. Never visible to read_committed consumers.
            "aborted_and_rewritten": int(everything.size - clean_seqs.size),
            "exact": clean.exact,
        }
        del clean_seqs, everything
        dlq, reasons = scan_topic(settings, DLQ_TOPIC, prefix, since_ms=since, reasons=True)
        out["events.dlq"] = {
            "rows": int(dlq.size),
            "injected": n_poison,
            "exact": int(dlq.size) == n_poison,
            "reasons": dict(reasons),
        }
        late_seen, _ = scan_topic(settings, LATE_TOPIC, prefix, since_ms=since)
        found = np.isin(late, late_seen)
        out["events.late"] = {
            "rows": int(late_seen.size),
            "unique": int(np.unique(late_seen).size),
            "sent_late": int(late.size),
            "sent_late_not_in_side_output": int((~found).sum()),
            "in_side_output_not_sent_late": int((~np.isin(late_seen, late)).sum()),
            "matches_ledger": bool(found.all()) and int(late_seen.size) == int(late.size),
        }

    if "clickhouse" in layers:
        ch = clickhouse(settings)
        raw_rows = ch_seqs(ch, "lumen.events", prefix, final=False).size
        layer = tally(ch_seqs(ch, "lumen.events", prefix, final=True), ledger)
        out["clickhouse.events"] = {
            **layer.__dict__,
            "rows_before_merge": int(raw_rows),
            "exact": layer.exact,
        }
        lo, hi = ledger["started_at_ms"], ledger["ended_at_ms"] + 15_000
        windowed = ch.query(
            "SELECT sum(events) FROM lumen.window_counts FINAL "
            "WHERE window_start >= fromUnixTimestamp64Milli({lo:Int64}) - INTERVAL 10 SECOND "
            "AND window_start < fromUnixTimestamp64Milli({hi:Int64})",
            parameters={"lo": lo, "hi": hi},
        ).result_rows[0][0]
        late_rows = ch_seqs(ch, "lumen.late_events", prefix, final=True)
        on_time = n_expected - int(late.size)
        out["windows_vs_late"] = {
            "on_time_expected": on_time,
            "counted_in_windows": int(windowed or 0),
            "late_expected": int(late.size),
            "late_side_output": int(np.unique(late_rows).size),
            "exact": int(windowed or 0) == on_time
            and int(np.unique(late_rows).size) == int(late.size),
        }
        dlq_rows = ch.query(
            "SELECT count() FROM lumen.dlq FINAL WHERE startsWith(key, {p:String})",
            parameters={"p": prefix},
        ).result_rows[0][0]
        out["clickhouse.dlq"] = {"rows": int(dlq_rows), "exact": int(dlq_rows) == n_poison}

    if "iceberg" in layers:
        # Late events carry event times up to 5 minutes before the run started.
        ice = tally(iceberg_seqs(settings, prefix, since - 600_000), ledger)
        out["iceberg.events"] = {**ice.__dict__, "exact": ice.exact}

    report["exact"] = all(bool(v.get("exact", v.get("matches_ledger", True))) for v in out.values())
    return report


def latency(settings: Settings, ledger_path: Path) -> dict[str, Any]:
    """Event time to queryable in ClickHouse, for events sent on time and in order."""
    ledger = json.loads(ledger_path.read_text())
    run = ledger["run_id"]
    prefix = f"{run}-"
    result = clickhouse(settings).query_np(
        f"SELECT toInt64OrDefault(substring(event_id, {len(prefix) + 1}), toInt64(-1)), "
        "toUnixTimestamp64Milli(min(_ingested_at)) - toUnixTimestamp64Milli(any(event_time)) "
        "FROM lumen.events WHERE startsWith(event_id, {p:String}) GROUP BY event_id",
        parameters={"p": prefix},
    )
    if result.size == 0:
        return {"run_id": run, "n": 0}
    seqs = np.asarray(result[:, 0], dtype=np.int64)
    ms = np.asarray(result[:, 1], dtype=np.float64)
    skip = np.array(ledger["late"] + ledger["out_of_order"], dtype=np.int64)
    lat = ms[~np.isin(seqs, skip)] / 1000
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
