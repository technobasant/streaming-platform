# 4. Exactly-once by transactions upstream, idempotence at the edge

Status: accepted · 2026-10

## Context

"Exactly once" has to hold from the producer to a row someone queries, through
broker loss, a TaskManager crash and a stalled job. Each hop needs its own reason to
neither lose nor duplicate.

## Decision

| Hop | Mechanism |
|---|---|
| generator → Kafka | idempotent producer, `acks=all`, `min.insync.replicas=2` |
| gate | consume-transform-produce in one Kafka transaction (ADR 2) |
| Flink → Kafka (`events.clean`, aggregates, late events) | Flink's two-phase-commit Kafka sink: a transaction per checkpoint, committed when the checkpoint completes |
| Flink → Iceberg | the Iceberg sink commits a snapshot per checkpoint |
| Kafka → ClickHouse | Kafka engine with `isolation_level=read_committed` into a `ReplacingMergeTree` keyed on the event |
| duplicates the producer sends on purpose | Flink deduplicates on `event_id` (ADR 6) |

ClickHouse's Kafka engine is at-least-once — it commits offsets after inserting a
block, so a crash between the two re-reads the block. The table's sort key includes
`event_id`, so a re-read is a duplicate row that `FINAL` (or the next merge)
collapses. The reconciler counts rows both ways and reports both.

## Why not a JDBC upsert sink or the ClickHouse Flink connector

Flink's JDBC connector has no ClickHouse dialect, and the ClickHouse connector is
DataStream-only. Going through Kafka keeps every Flink sink transactional and gives
ClickHouse a replayable source: dropping and recreating a table re-reads the topic.

## Consequences

* **Latency has a floor: the checkpoint interval.** Output becomes visible only when
  the transaction commits, so with checkpoints every 5 s a record waits 0–5 s before
  anyone can read it. MEASUREMENTS.md shows the median end-to-end latency sitting
  just above half that interval. A shorter interval buys latency with more
  transactions, more checkpoint I/O and more small Iceberg files.
* Every `read_committed` consumer's lag includes transaction markers, so lag never
  reads exactly zero.
* The Flink sink's `transaction.timeout.ms` (10 min) must stay below the brokers'
  `transaction.max.timeout.ms` (15 min), and a job stopped for longer than it loses
  its open transactions.
