# 5. Iceberg on MinIO through the REST catalog fixture

Status: accepted · 2026-10

## Context

The lake copy needs a catalog Flink can commit to. The candidates were Hive
Metastore (plus its database), Nessie, Apache Polaris, a JDBC catalog straight from
Flink, and a REST catalog.

## Decision

`apache/iceberg-rest-fixture` 1.9.2: Iceberg's reference REST catalog server, backed
by SQLite inside the container, with `S3FileIO` against MinIO. Flink uses the REST
catalog type from `iceberg-flink-runtime`; the reconciler reads the same table with
PyIceberg.

## Why

It is one small JVM (256 MB heap) with no database to run, and the REST protocol is
what Polaris, Nessie, Unity, Glue and S3 Tables all speak, so the Flink side is the
production configuration with a different URI. Hive Metastore would add a JVM and
PostgreSQL for features nothing here uses.

## Consequences

* The fixture is a test server: no authentication, no credential vending. A real
  deployment points the same `CREATE CATALOG` at Polaris or a managed catalog
  (the lakehouse-lab repository runs Polaris).
* The SQLite file survives a container restart, not `make down`.
* The sink commits a snapshot per checkpoint, every 5 s: many small files. Nothing
  here compacts them (Limits).
