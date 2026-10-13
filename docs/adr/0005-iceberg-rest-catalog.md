# 5. Iceberg through the REST catalog fixture, on SeaweedFS S3

Status: accepted · 2026-10

## Context

The lake copy needs a catalog Flink can commit to. The candidates were Hive
Metastore (plus its database), Nessie, Apache Polaris, a JDBC catalog straight from
Flink, and a REST catalog.

## Decision

`apache/iceberg-rest-fixture` 1.9.2: Iceberg's reference REST catalog server, backed
by SQLite inside the container, with `S3FileIO` against SeaweedFS's S3 gateway. Flink uses the REST
catalog type from `iceberg-flink-runtime`; the reconciler reads the same table with
PyIceberg.

## Why

It is one small JVM (256 MB heap) with no database to run, and the REST protocol is
what Polaris, Nessie, Unity, Glue and S3 Tables all speak, so the Flink side is the
production configuration with a different URI. Hive Metastore would add a JVM and
PostgreSQL for features nothing here uses.

## Why SeaweedFS for S3

The first version used MinIO. Its images are no longer published (the `minio/minio`
repository is gone from Docker Hub), so a stack pinned to one could not be rebuilt by
anyone else. SeaweedFS is Apache-2.0, publishes multi-arch images, and its `weed mini`
mode runs master, volume server, filer and S3 gateway in one small process with the
buckets created at start. Every measurement in MEASUREMENTS.md was taken on it.

## Consequences

* The fixture is a test server: no authentication, no credential vending. A real
  deployment points the same `CREATE CATALOG` at Polaris or a managed catalog
  (the lakehouse-lab repository runs Polaris).
* The SQLite file survives a container restart, not `make down`.
* The sink commits a snapshot per checkpoint, every 5 s: many small files. Nothing
  here compacts them (Limits).
