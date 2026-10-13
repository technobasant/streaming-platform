# 2. A transactional contract gate in front of Flink

Status: accepted · 2026-10

## Context

Producers send Avro in the Confluent wire format. Some messages will be bad: not
Avro, written with a schema the registry does not know, truncated, or valid Avro that
breaks a business rule (a payment without an amount). Each must end up somewhere a
person can see it, with the reason, and none may stop the pipeline.

Flink SQL's `avro-confluent` format has two behaviours on a bad record: fail the job,
or (`ignore-parse-errors`) drop it silently. Neither routes it anywhere, and a
format cannot apply business rules.

## Decision

A small Python service, the gatekeeper, between `clickstream.raw` and Flink:

* Consumes `read_committed`, decodes with the writer's schema from the registry and
  resolves to the reader schema it was built against (v1), then applies the business
  rules in `validation.py`.
* Writes valid records as JSON to `events.valid`, keyed by `event_id`, and bad ones
  byte-for-byte to `events.dlq` with the reason and source offset in headers.
* Does both, and commits its input offsets, in one Kafka transaction per batch
  (`send_offsets_to_transaction`). A crash replays the batch; the aborted copy is
  never visible to `read_committed` readers.

## Why not a Flink DataStream job with a side output

That would keep everything in Flink, and is what a larger team should do. It would
also put a Java build into a repository whose jobs are otherwise SQL files, for one
stage. The gatekeeper is about 150 lines, unit-tested for every kind of poison, and
its transaction is the same mechanism Flink's sink uses.

## Consequences

* One more hop. It adds the batch linger (50 ms) and a transaction commit per batch
  to end-to-end latency, small next to Flink's checkpoint interval.
* One instance with a fixed `transactional.id`. Scaling out needs one id per
  instance and partition assignment that keeps them stable — not done here.
* Contract enforcement happens twice: at registration (the registry refuses
  incompatible schemas) and at runtime (producers cannot encode with a schema the
  registry does not hold; the gate catches whatever arrives anyway).
