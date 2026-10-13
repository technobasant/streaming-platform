# Changelog

## 0.1.0 — 2026-10

First release.

- A compose stack, one container per node: Apache Kafka 4.1 (three KRaft nodes,
  RF 3, `min.insync.replicas=2`), Confluent Schema Registry 8.0, Flink 1.20.2 (one
  JobManager, two TaskManagers, RocksDB, checkpoints every 5 s to MinIO), ClickHouse
  25.8, Iceberg on MinIO through the Iceberg REST catalog, Prometheus and Grafana.
  `make up`, `make demo`, `make down`.
- The contract: `ClickstreamEvent` v1 in Avro under `FULL_TRANSITIVE`, a compatible
  v2, and three breaking variants that the registry must refuse. Producers never
  auto-register.
- `streamplat`, the Python tooling: a seeded generator of storefront traffic for the
  fictional retailer Lumen & Loom, with rate control, late, out-of-order, duplicate
  and four kinds of poison events and a ledger of each; the gatekeeper, which routes
  raw Avro to `events.valid` or `events.dlq` in one Kafka transaction per batch; the
  freshness probe exporter; a reconciler that counts a run at every layer against
  its ledger; and the contract check.
- Flink SQL jobs: `clean` (deduplicate on `event_id`; exactly-once to Kafka and
  Iceberg) and `windows` (10 s tumbling counts, 30 s-gap sessions with funnel stage,
  late events to a side output).
- ClickHouse: Kafka engine tables reading `read_committed`, materialized views into
  `ReplacingMergeTree` tables for events, windows, sessions, late events and the
  dead-letter queue.
- A freshness SLO (99% of probes queryable within 15 s) with multi-window burn-rate
  alerts, and a provisioned Grafana dashboard.
- `scripts/measure.py` and MEASUREMENTS.md: load levels, a broker kill, a
  TaskManager kill, a stalled job against the alert, schema rejection, a v2
  rollout, ClickHouse query latency.
- CI: ruff, mypy, pytest; compose and alert-rule validation; the contract test
  against a live registry; an end-to-end run of the full stack.
