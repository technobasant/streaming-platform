# 3. Avro with FULL_TRANSITIVE compatibility

Status: accepted · 2026-10

## Context

Producers (the web app, the public API) and consumers (the gate, Flink, the lake)
deploy on their own schedules, and the Iceberg table keeps years of history that a
new reader must still read.

## Decision

* Avro, registered in Confluent Schema Registry under `clickstream.raw-value`.
* `FULL_TRANSITIVE`: every new version must be both backward and forward compatible
  with every earlier one. In practice: add or remove only fields that have defaults;
  never change a type.
* Producers do not auto-register. A schema reaches the registry through review
  (here, `contracts/` and the contract test in CI), and a producer built on any other
  schema fails before it sends a byte.

## Why not BACKWARD

BACKWARD lets consumers upgrade first, which is the common case, but it allows
removing a field without a default — and an old consumer, or a replay of old code
over new data, then breaks. FULL costs little for an event stream (new fields are
optional anyway) and removes the ordering question entirely.

## Why not Protobuf

Protobuf would work with the same registry. Avro's schema resolution is explicit —
the reader's schema and the writer's are both known and reconciled field by field —
which is exactly what the compatibility tests exercise offline without a registry.

## Consequences

* The three breaking changes in `contracts/clickstream/breaking/` are refused with
  HTTP 409, and the registry's reason names the field (MEASUREMENTS.md).
* Enum symbols cannot be added without a default symbol under FULL. When a new event
  type arrives, the enum gets a default first.
