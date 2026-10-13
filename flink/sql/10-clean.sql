-- Job 1 — clean: deduplicate events.valid on event_id, then write the result twice,
-- in one job: to events.clean (Kafka, transactional) and to the Iceberg table.
-- Both sinks commit on Flink checkpoints, so both are exactly-once.

SET 'pipeline.name' = 'clean';
SET 'parallelism.default' = '2';
-- Deduplication state per event_id. Duplicates in this system arrive within seconds;
-- thirty minutes is the horizon, and the cost, of the guarantee.
SET 'table.exec.state.ttl' = '30 min';

CREATE TABLE events_valid (
  event_id     STRING,
  event_type   STRING,
  event_time   TIMESTAMP(3),
  session_id   STRING,
  customer_id  STRING,
  client       STRING,
  page         STRING,
  product_id   STRING,
  quantity     INT,
  order_id     STRING,
  amount_cents BIGINT,
  currency     STRING,
  is_probe     BOOLEAN,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'events.valid',
  'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
  'properties.group.id' = 'flink-clean',
  'properties.isolation.level' = 'read_committed',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE events_clean (
  event_id     STRING,
  event_type   STRING,
  event_time   TIMESTAMP(3),
  session_id   STRING,
  customer_id  STRING,
  client       STRING,
  page         STRING,
  product_id   STRING,
  quantity     INT,
  order_id     STRING,
  amount_cents BIGINT,
  currency     STRING,
  is_probe     BOOLEAN
) WITH (
  'connector' = 'kafka',
  'topic' = 'events.clean',
  'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
  'key.format' = 'raw',
  'key.fields' = 'event_id',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'SQL',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'sp-clean',
  -- Must not exceed the brokers' transaction.max.timeout.ms (15 min by default).
  'properties.transaction.timeout.ms' = '600000'
);

CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://seaweedfs:8333',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'lumen',
  's3.secret-access-key' = 'lumen-lab-only',
  'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lake.clickstream;

CREATE TABLE IF NOT EXISTS lake.clickstream.events (
  event_id     STRING,
  event_type   STRING,
  event_time   TIMESTAMP(3),
  event_date   DATE,
  session_id   STRING,
  customer_id  STRING,
  client       STRING,
  page         STRING,
  product_id   STRING,
  quantity     INT,
  order_id     STRING,
  amount_cents BIGINT,
  currency     STRING
) PARTITIONED BY (event_date) WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'zstd'
);

-- Keep the first arrival of each event_id. Ordering by processing time makes the
-- result append-only, which both sinks need; ordering by event time would emit
-- retractions whenever an earlier copy arrived late.
CREATE TEMPORARY VIEW deduped AS
SELECT event_id, event_type, event_time, session_id, customer_id, client, page,
       product_id, quantity, order_id, amount_cents, currency, is_probe
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) AS rn
  FROM events_valid
)
WHERE rn = 1;

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO events_clean SELECT * FROM deduped;
  INSERT INTO lake.clickstream.events
  SELECT event_id, event_type, event_time, CAST(event_time AS DATE), session_id,
         customer_id, client, page, product_id, quantity, order_id, amount_cents, currency
  FROM deduped
  WHERE NOT is_probe;
END;
