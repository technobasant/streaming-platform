# 6. Deduplication by arrival, windows by event time, late data kept

Status: accepted · 2026-10

## Context

Clients resend events (retries) and send them late (a phone back online) and out of
order. Counts per window must be right, duplicates must not count twice, and a late
event must not disappear.

## Decision

* **Deduplicate first**, in the `clean` job: `ROW_NUMBER() OVER (PARTITION BY event_id
  ORDER BY proc_time)`, keep row 1, state TTL 30 minutes. Ordering by processing time
  makes the output append-only, which the transactional Kafka and Iceberg sinks need.
* **Windows on event time**, in the `windows` job, reading the deduplicated stream:
  watermark = newest event time seen minus 5 s; 10-second tumbling windows; sessions
  with a 30 s gap and the furthest funnel stage reached.
* **Late data to a side output.** A window drops a record once the watermark has
  reached the last millisecond of the record's window. A separate statement routes
  exactly those records — same condition, `window_end - 1 ms <= CURRENT_WATERMARK` —
  to `events.late`. Every event lands in exactly one place, and the reconciler checks
  that it did.
* The raw `events` table in ClickHouse and the Iceberg table keep every unique event,
  late or not. Only the window aggregates exclude late ones.

## Why not allowed lateness with updating windows

Updating results would turn the window output into a changelog, which neither the
append-only Kafka sink nor `ReplacingMergeTree` keyed on the window handle without
retractions. A late event three minutes after its window closed is a correction for a
batch backfill, not a reason to make every consumer handle updates.

## Consequences

* A duplicate that arrives more than 30 minutes after the original is counted twice.
* The out-of-order tolerance (5 s) is also a latency floor for the aggregates: a
  window closes 5 s after its end at the earliest, plus a checkpoint.
* The side-output condition uses the watermark of the source task that sees the
  record; the window operator's watermark can lag it slightly. MEASUREMENTS.md
  reports whether the two ever disagreed.
