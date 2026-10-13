# 1. Apache Kafka in KRaft mode, three brokers

Status: accepted · 2026-10

## Context

The platform's guarantees rest on the log: idempotent producers, transactions that
commit consumer offsets and output together, and `read_committed` consumers. The
cluster has to survive the loss of a broker without losing an acknowledged write,
on a Docker VM of 8 CPUs and about 12 GB shared with everything else here.

## Decision

* Apache Kafka 4.3.1 (`apache/kafka`), three nodes, each both broker and KRaft
  controller, one container per node.
* Every topic: replication factor 3, `min.insync.replicas=2`, unclean leader election
  off. Producers use `acks=all` with idempotence on. Topics are created by the
  bootstrap step; auto-creation is off.
* 512 MB heap per broker.

## Why not Redpanda

Redpanda would be lighter (no JVM, one binary) and speaks the same protocol,
transactions included. It lost on two counts. Flink's exactly-once sink and the
librdkafka transactional client are developed and tested against Apache Kafka, and
the failure behaviour measured here — a controller and a third of the partition
leaders dying at once — should be Kafka's, since that is what most readers run.
And Redpanda's core is under the Business Source License; everything in this
repository is Apache-2.0 or compatible.

## Why combined broker and controller

Separate controllers are the production layout. Three more JVMs would not fit, and
combined mode still exercises what matters here: killing the active controller
forces a KRaft leader election and partition leadership changes at the same time.

## Why 4.3 and not 4.1

The first runs used 4.1.0 and hit KAFKA-19888: the group coordinator measures
durations with the wall clock, the Docker VM's clock stepped backwards, a metrics
histogram threw on the negative value, and the coordinator for one
`__consumer_offsets` partition stopped accepting writes until a restart. The
ClickHouse consumer group on that partition sat in `CompletingRebalance` for over
fifteen minutes and the dead-letter table stopped filling. Fixed in 4.1.2 and 4.2.0;
this uses 4.3.1.

## Consequences

* Losing one node is survivable for writes (two in-sync replicas remain). Losing two
  stops writes — by design — and also loses the KRaft quorum.
* All three nodes share one host. Replication latency is local, and the broker-kill
  measurement says nothing about network partitions.
