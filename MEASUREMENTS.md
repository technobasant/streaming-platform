# Measurements

Every figure here comes from a run of the stack in this repository, driven by
`scripts/measure.py`. Raw output, including every reconciliation report, is in
[`results/`](results). Timings are medians of three runs unless a table lists each run.

## Environment

| | |
|---|---|
| Host | Apple M5, 10 cores, 24 GiB; Docker Desktop 29.8.2, a Linux VM (7.0.14, arm64) with 8 vCPU and 11.7 GiB of memory |
| Shared host | Every node is a container on that one VM, so replication and network latency are local and a stall on the host stalls every node. Unrelated containers ran alongside, using about 0.4 GiB and little CPU. |
| Kafka | Apache Kafka 4.3.1, three KRaft nodes (broker and controller each), 512 MB heap; RF 3, `min.insync.replicas=2`; six partitions on the hot path |
| Registry | Confluent Schema Registry 8.0.0, `FULL_TRANSITIVE` |
| Flink | 1.20.2: one JobManager, two TaskManagers × 4 slots, 1.6 GB each; checkpoints every 5 s, exactly-once, to S3; `clean` at parallelism 2 on RocksDB, `windows` at parallelism 1 on heap state; Kafka connector 3.4.0, Iceberg 1.10.1 |
| Sinks | ClickHouse 25.8.33.6 (1.5 GB); Iceberg through `apache/iceberg-rest-fixture` 1.9.2 on SeaweedFS 4.47 |
| Tools | Python 3.12, confluent-kafka 2.16.0 (librdkafka 2.16.0), one gatekeeper process |
| Monitoring | Prometheus 3.5.0 (5 s scrape and rule evaluation), Grafana 12.1.1 |

## Method

**Traffic.** The generator replays a seeded storefront: 200 concurrent shopper sessions,
each a walk down the funnel (page views → cart → checkout → payment, sometimes a failed
payment and a retry), from the web app or the API. On top of that, per event:

| Fault | Rate | What it is |
|---|---|---|
| late | 1% | event time 180–300 s in the past (a client that was offline) |
| out of order | 5% | held 0.2–3 s before sending; event time unchanged |
| duplicate | 2% | the same bytes sent again 0–2 s later |
| poison | 0.1% | one of: not Avro (a JSON body), an unknown schema id, a truncated body, or valid Avro that breaks a business rule |

Every send is recorded in a ledger by sequence number. Event ids are `<run>-<seq>`.

**Reconciliation.** After a run, `streamplat reconcile` counts the run's events at every
layer and compares the set of sequence numbers with the ledger: `events.valid` (every
send, duplicates included), `events.clean` (each event once), the dead-letter topic
(exactly the poison), `events.late` (exactly the late events), ClickHouse's `events`
table, the Iceberg table, the sum of the 10-second window counts over the run, and
ClickHouse's dead-letter table. A layer is *exact* when nothing is missing, nothing
is there twice, and no poison leaked. Kafka is read `read_committed`; it is also read
`read_uncommitted`, and the difference is what aborted transactions wrote before a
restart redid it.

**Latency.** End-to-end latency is ClickHouse's `_ingested_at` (set when the
materialized view inserts the row) minus the event time, for every event that was
sent on time and in order — late and held-back events are excluded, because their
event time is not when they were sent. All clocks are the same VM's.

## 1. Throughput and end-to-end latency

Three offered rates, 90 s each, three runs per rate, with every fault above.

| rate (events/s) | sent | unique valid | p50 s | p95 s | p99 s | max s | DLQ / poison | duplicates sent | late in side output / sent | lost / duplicated |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 89,999 | 89,918 | 3.49 | 5.71 | 6.12 | 6.62 | 81 / 81 | 1,837 | 913 / 913 | 0 / 0 |
| 1,000 | 89,999 | 89,918 | 3.40 | 5.71 | 6.08 | 6.49 | 81 / 81 | 1,837 | 913 / 913 | 0 / 0 |
| 1,000 | 89,998 | 89,917 | 3.39 | 5.67 | 6.03 | 6.46 | 81 / 81 | 1,837 | 913 / 913 | 0 / 0 |
| 3,000 | 269,994 | 269,734 | 3.71 | 6.00 | 6.59 | 6.98 | 260 / 260 | 5,446 | 2,744 / 2,744 | 0 / 0 |
| 3,000 | 269,996 | 269,736 | 3.75 | 6.10 | 6.71 | 7.27 | 260 / 260 | 5,446 | 2,744 / 2,744 | 0 / 0 |
| 3,000 | 269,989 | 269,729 | 3.72 | 6.04 | 6.54 | 6.96 | 260 / 260 | 5,446 | 2,744 / 2,744 | 0 / 0 |
| 6,000 | 539,991 | 539,430 | 4.31 | 6.73 | 7.57 | 8.10 | 561 / 561 | 10,889 | 5,413 / 5,413 | 0 / 0 |
| 6,000 | 539,991 | 539,430 | 5.00 | 8.32 | 9.66 | 10.43 | 561 / 561 | 10,889 | 5,413 / 5,413 | 0 / 0 |
| 6,000 | 539,379 | 538,818 | 4.95 | 8.16 | 9.05 | 9.71 | 561 / 561 | 10,882 | 5,407 / 5,407 | 0 / 0 |

| rate | p50 | p95 | p99 | max (worst run) |
|---:|---:|---:|---:|---:|
| 1,000/s | 3.40 s | 5.71 s | 6.08 s | 6.62 s |
| 3,000/s | 3.72 s | 6.04 s | 6.59 s | 7.27 s |
| 6,000/s | 4.95 s | 8.16 s | 9.05 s | 10.43 s |

* **The floor is the checkpoint interval.** Output becomes readable only when Flink's
  transaction commits, every 5 s, so an event waits 0–5 s for that alone: the median sits
  just above half the interval at every rate. Add the gate's 50 ms batch, ClickHouse's
  500 ms flush and its Kafka engine's polling.
* **6,000 events/s is near the edge.** The `clean` job (deduplication on RocksDB) peaked
  at about 6,100 records/s; at 6,000/s its consumer lag grew during the run and drained
  after it, which is the extra second or two at p95 and p99. That peak comes from one
  separate 60 s run at 6,000/s; no higher rate was measured on the final stack.
* These runs were taken with the `windows` job at parallelism 2; it is now 1 (section 3
  says why). That job is not on the path these latencies measure — `clean` →
  `events.clean` → ClickHouse — and a 6,000/s check afterwards kept up at parallelism 1.

**After four hours, it was slower.** A repeat of the load runs at the end of all the
experiments, on the same stack (about four hours and 6 million rows later, after nine
fault runs), gave at 1,000 events/s p50 4.0 and 3.6 s, but p95 21.4 and 6.8 s and p99
25.0 and 22.8 s, still exact at every layer. Flink's checkpoints for `clean` then had a
p99 of 5.1 s and a maximum of 32 s, and SeaweedFS sat at its 512 MB memory limit; the
Iceberg table had only 655 snapshots, so metadata growth is not the obvious cause. Not
root-caused; the repeat was stopped after two runs (`results/load-after-4h.json`).

## 2. Exactly-once in steady state

All nine load runs: **every layer exact.** 2.7 million unique events in total; for each
run, `events.valid` held every send including the duplicates, `events.clean`, ClickHouse
and Iceberg each held every valid event exactly once, the dead-letter topic and table
held exactly the poison, `events.late` held exactly the late events, and the window
counts summed to exactly the on-time events. No aborted transactions in steady state,
and ClickHouse needed no `FINAL` to be exact (rows before merge = rows after).

## 3. Killing a Kafka broker under load

2,000 events/s for 100 s. At 30 s the broker that is the active KRaft controller is
killed with SIGKILL (so a third of partition leaders and the controller go at once);
30 s later it is started again.

| run | killed | producer failures | max ack latency before / after kill | seconds with 0 acks | ISR full again after restart | e2e p50 / p99 | event layers | on-time events that became late |
|---|---|---:|---|---:|---:|---|---|---:|
| 1 | kafka-1 (controller) | 0 | 0.6 s / 11.8 s | 10 | 21 s | 5.7 / 39.4 s | exact | 4 |
| 2 | kafka-2 (controller) | 0 | 0.5 s / 11.4 s | 10 | 23 s | 6.2 / 48.1 s | exact | 38,023 |
| 3 | kafka-1 (controller) | 0 | 0.7 s / 11.7 s | 9 | not observed (health exporter restarting) | 36.2 / 81.3 s | exact | 79,520 |

* **Nothing lost, nothing duplicated, at any layer, in any run** — `events.valid`,
  `events.clean`, ClickHouse, Iceberg and the dead-letter topic were all exact. The
  idempotent producer retried through the failover with no failed sends.
* **Writes stop for about 10 s.** Every partition has the dead broker in its ISR (RF 3
  on three brokers), and `acks=all` waits for the whole ISR until the controller fences
  the broker (`broker.session.timeout.ms`, 9 s) and shrinks it.
* **The gate stalled longest when its transaction coordinator died.** In run 3 the
  killed broker was the gatekeeper's transaction coordinator: librdkafka spent its
  default 30 s connection-setup timeout on the dead host, the transaction then failed,
  the abort failed too (Docker had already removed the host name), and the process
  exited. Docker restarted it; the new process fenced the old one and resumed from the
  last committed offsets — still exactly-once, but the pipeline waited about 70 s, which
  is that run's p50 of 36 s. The connection-setup timeout is now 5 s
  (commit c1f5e85); that change has **not** been re-measured.
* **Aggregates.** Windows plus the late side output summed to exactly the run's events
  in every run: no event lost from both, none counted in both. But a failover moves
  on-time events to the side output — 4, 38,023 and 79,520 of about 200,000. When the
  pipeline catches up after a stall it reads the six partitions at different speeds,
  and re-keying by `event_id` mixes them, so events.clean carries more than the 5 s
  of disorder the watermark allows. The raw table and the lake are unaffected; the
  window aggregates under-count until the side output is applied.

## 4. Killing a Flink TaskManager under load

2,000 events/s for 90 s; at 30 s the TaskManager running the most slots is killed with
SIGKILL. The other TaskManager has spare slots, so both jobs can restart on it; the
killed one is started again once they are running.

| run | slots on the killed TM | failure noticed | state restored (checkpoint) | all tasks running | records rewritten after abort | e2e p99 / max | event layers | aggregates |
|---|---:|---:|---|---:|---:|---|---|---|
| 1 | 2 | 8.8 s | clean 10.6 s (#874) | 14 s | 3,556 | 25.2 / 26.7 s | exact | exact split; 10 on-time → late |
| 2 | 3 | 8.1 s | clean 10.9 s (#942), windows 9.8 s (#340) | 63 s | 6,647 | 21.6 / 22.5 s | exact | 241 late events in neither; 3 on-time → late |
| 3 | 3 | 8.0 s | clean 13.5 s (#1009), windows 10.1 s (#398) | 52 s | 7,522 | 26.7 / 27.6 s | exact | 95 late events in neither; 14,671 on-time → late |

* **Exactly-once held**: every event layer exact in all three runs. The `clean` job's
  sink had written 3,556–7,522 records inside transactions that the failure aborted;
  `read_committed` readers never saw them, and the restored job wrote them again.
* **Detection took 8.0–8.8 s**, inside the 10 s heartbeat timeout (heartbeats every 2 s),
  and state was restored 10–13 s after the kill. Every task was running again 14 s after
  the kill in run 1, where only `clean` restarted, and 52–63 s after it in runs 2 and 3,
  where both jobs did. Why the second job took that long was not investigated.
* **A loss in the aggregates, found here.** When the `windows` job is restored, its window
  operators restore their watermark but the source's `CURRENT_WATERMARK` starts empty. A
  late event read before the first new watermark is therefore not routed to
  `events.late` (the filter has no watermark to compare with) and is dropped by the
  window (which has one): 241 and 95 events in runs 2 and 3. They are still in the raw
  table and the lake. Fixing it properly needs the late events to come out of the
  window operator itself — a DataStream side output — which is on the roadmap.

## 5. A stalled job against the burn-rate alert

500 events/s for 330 s. At 60 s both TaskManagers are frozen with `docker pause` (the
processes stop; nothing crashes, nothing is notified) for 150 s, then unpaused. The
script polls Prometheus's alert state every second.

| run | fast burn pending | fast burn firing | jobs running after unpause | alert resolved after unpause | records rewritten after abort | event layers | aggregates |
|---|---:|---:|---:|---:|---:|---|---|
| 1 | 59 s | 59 s | 35 s | 44 s | 2,135 | exact | 20,194 on-time → late; 276 late in neither |
| 2 | 62 s | 62 s | 19 s | 46 s | 658 | exact | 39,376 on-time → late; 236 late in neither |
| 3 | 60 s | 61 s | 12 s | 45 s | 1,613 | exact | 29,357 on-time → late; 142 late in neither |

* **The page fires about 60 s into the stall** (59–62 s). Probes are judged 15 s after they
  are sent, and the 5-minute window (the compressed 1-hour window) needs more than 14.4%
  of its probes bad, which at one probe per 5 s takes about another 45 s. The 25 s window
  agrees well before that. In production, with the uncompressed 1 h/5 m pair, the same
  arithmetic gives about 9 minutes.
* **It clears about 45 s after the unpause**: the jobs take 12–35 s to restart from their
  last checkpoints (the JobManager had declared both TaskManagers lost after the 10 s
  heartbeat timeout), the backlog drains, probes land again, and the 25 s window drops
  below the threshold while the 5-minute one is still above it — the short window is
  what stops a recovered incident from paging on.
* **Exactly-once held through the stall** at every event layer. The aggregates did not:
  the catch-up after 150 s moved 20,000–39,000 on-time events to `events.late`, and the
  restore lost 142–276 late events from both windows and side output (section 4 explains
  both).


## 6. Late and out-of-order events

From the steady-state runs (section 1): every out-of-order event (5% of traffic, held up
to 3 s) was counted in its window — the window sums matched the on-time events exactly —
and every late event (1%, 180–300 s old) was in `events.late` and in no window: 913,
2,744 and 5,413 per run at the three rates, each matching the ledger. The failure runs
show the limits of that split (sections 3 and 4).

## 7. Dead-letter queue against injected poison

In every run of every experiment the dead-letter topic held exactly the poison the
generator injected, by count and by sequence: 81, 260 and 561 per run in the load runs,
with the reasons `not_confluent_avro`, `schema_not_found`, `avro_decode_error` and
`rule:<rule>` (unknown currency, non-positive cart quantity, non-positive payment). The
ClickHouse dead-letter table matched it in every run once Kafka 4.1.0 was replaced
(see "What broke").

## 8. Schema rejection

`streamplat contract-check`, three times against the live registry, each on a scratch
subject with `FULL_TRANSITIVE` and v1 registered:

| change | compatibility check | register | registry's reason |
|---|---|---|---|
| v2: add `referrer`, optional with a default | compatible | accepted | — |
| add `channel`, required, no default | incompatible | **409** | `READER_FIELD_MISSING_DEFAULT_VALUE`: `channel` has no default and is missing in the old schema |
| `amount_cents` long → string | incompatible | **409** | `MISSING_UNION_BRANCH` in both directions |
| remove `session_id` (no default) | incompatible | **409** | `READER_FIELD_MISSING_DEFAULT_VALUE`: `session_id` is missing in the new schema |

All three runs gave the same verdicts; a refused registration took a median of
3.5 ms. A producer built on the first breaking schema against the
live subject never sent anything — it fails while looking its schema up:
`streamplat.registry.RegistryError: HTTP 404 (error_code 40403): Schema not found`. CI runs the same check on every
push (`pytest -m registry`).


## 9. ClickHouse queries on fresh data

While the generator sends 2,000 events/s, five dashboard queries run 20 times each over
HTTP from the host, 60 s into the run; three runs, with the `events` table at 5.6–6.1
million rows. Times are client-side and include HTTP.

| query | p50 ms | p95 ms |
|---|---:|---:|
| funnel counts and revenue, last 5 min (`events FINAL`) | 24 | 77 |
| top 10 products added to cart, last 5 min (`events FINAL`) | 13 | 70 |
| 10-second window counts, last 15 min (`window_counts FINAL`) | 8 | 30 |
| conversion by client, last 15 min (`sessions FINAL`) | 26 | 84 |
| age of the newest event (`max(event_time)`) | 21 | 82 |

* The newest event was 4.1, 4.7 and 6.9 s old when queried — the same checkpoint-bound
  freshness as section 1, seen from the reader's side.
* `FINAL` costs something on a table that is still being written: the funnel query took
  15 ms without it and 24 ms with it (p50). Without `FINAL` the answer was the
  same in these runs, because the Kafka engine did not re-deliver anything; after a
  ClickHouse restart it could, and then only `FINAL` (or a merge) is right.

## 10. A compatible schema change, live

v2 (v1 plus an optional `referrer`) registered on the live subject, then 30 s of traffic
at 1,000 events/s produced with v2 while the gatekeeper still reads with v1: all 29,970
events exact at every layer, and the dead-letter topic held only the 29 injected poison
messages — none of the v2 records was refused.


## What broke, and what changed

| What happened | Why | Change |
|---|---|---|
| Brokers idled at ~150% CPU each, four of the host's eight CPUs | The healthcheck started a JVM (`kafka-broker-api-versions.sh`) every 5 s on each broker | A port check; brokers idle at ~5% (5e7c2b0). Every earlier measurement was discarded. |
| The `windows` job topped out near 2,800 events/s and fell minutes behind at 6,000/s | RocksDB state access per record, for state that is small and short-lived | Heap state for that job (f0eb519) |
| The ClickHouse dead-letter table stopped filling while the topic was complete; its consumer group sat in `CompletingRebalance` for 15+ minutes | Kafka 4.1.0's group coordinator measures durations with the wall clock; the VM's clock stepped back, a histogram threw, and the coordinator for one `__consumer_offsets` partition timed out every write after that ([KAFKA-19888](https://issues.apache.org/jira/browse/KAFKA-19888)) | Kafka 4.3.1 (65e3d70) |
| One broker kill turned 1.7% of a run's on-time events late | Stalled partitions were declared idle after 10 s and the watermark moved on without them | Idle timeout 60 s (19c2903) |
| A broker kill counted 452 events both in a window and in `events.late` | With two source subtasks, the late filter and the windows saw different watermarks | `windows` at parallelism 1 (3673d7b) |
| The exporter died in two of three broker kills, and probes stopped for ~10 s | librdkafka 2.16.0 aborts in an admin request that times out while its coordinator is unreachable (`rd_kafka_enq_once_del_source_return: Assertion 'eonce->refcnt > 0'`, [librdkafka#4605](https://github.com/confluentinc/librdkafka/issues/4605)) | Lag and replication moved to a separate process; the prober no longer makes admin calls (184804c). The Kafka-health process still crashes and restarts during broker kills. |
| The reconciler was OOM-killed at 300 MB, then at 768 MB | It held every event id as a Python string, several times over, and read the whole Iceberg table on every retry | Counts per sequence number with numpy; event-time pruning of the Iceberg scan; only failing layers reread (bda5414, dc9a31d, 00d120a) |
| The pinned MinIO image could no longer be pulled | `minio/minio` is gone from Docker Hub | SeaweedFS (7491ca3) |
| A Grafana dashboard provisioned at the root was denied to the anonymous viewer | Grafana 12 permissions | Provisioned into a folder (ab0231e) |

Before the first commit: Flink SQL has no `MILLISECOND` unit for `TIMESTAMPADD`, and
`sql-client.sh -f` exits 0 when a statement fails, so the submit script reported "all jobs
running" with one job missing (it now fails on `[ERROR]`); ClickHouse's entrypoint writes
into `users.d`, so mounting that directory read-only crash-looped the server.

## Not measured, or measured once

* Network faults (partitions, latency): every node shares one VM. QEMU VMs are on the
  roadmap.
* Losing two brokers (writes should stop under `min.insync.replicas=2`; the KRaft quorum
  is lost too).
* The gatekeeper's 5 s connection-setup timeout, added after the broker-kill runs.
* Sustained load beyond 90 s per run, and the cause of the slowdown after four hours
  (section 1).
* The load runs with the `windows` job at parallelism 1 (they used 2; see section 1).
