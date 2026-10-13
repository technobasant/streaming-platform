-- Kafka engine tables read committed Flink output; materialized views move each
-- block into a ReplacingMergeTree. The Kafka engine is at-least-once: it commits
-- offsets after the insert, so a crash between the two re-reads a block. The
-- ReplacingMergeTree key makes that harmless — query with FINAL, or let merges
-- collapse the copies.

CREATE DATABASE IF NOT EXISTS lumen;

-- Every unique event, on time or late, plus the freshness probes (is_probe).
CREATE TABLE lumen.events
(
    event_id      String,
    event_type    LowCardinality(String),
    event_time    DateTime64(3, 'UTC'),
    session_id    String,
    customer_id   Nullable(String),
    client        LowCardinality(String),
    page          Nullable(String),
    product_id    Nullable(String),
    quantity      Nullable(Int32),
    order_id      Nullable(String),
    amount_cents  Nullable(Int64),
    currency      LowCardinality(String),
    is_probe      Bool,
    _ingested_at  DateTime64(3, 'UTC'),
    _partition    UInt16,
    _offset       UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_type, event_time, event_id);

CREATE TABLE lumen.events_queue
(
    event_id String, event_type String, event_time DateTime64(3, 'UTC'), session_id String,
    customer_id Nullable(String), client String, page Nullable(String),
    product_id Nullable(String), quantity Nullable(Int32), order_id Nullable(String),
    amount_cents Nullable(Int64), currency String, is_probe Bool
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'events.clean',
         kafka_group_name = 'clickhouse-events',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_flush_interval_ms = 500,
         kafka_max_block_size = 65536;

CREATE MATERIALIZED VIEW lumen.events_mv TO lumen.events AS
SELECT *, now64(3, 'UTC') AS _ingested_at, _partition, _offset
FROM lumen.events_queue;

-- 10-second tumbling windows, emitted once each by Flink when the watermark passes.
CREATE TABLE lumen.window_counts
(
    window_start  DateTime64(3, 'UTC'),
    window_end    DateTime64(3, 'UTC'),
    event_type    LowCardinality(String),
    events        UInt64,
    amount_cents  Int64,
    sessions      UInt64,
    _ingested_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (window_start, event_type);

CREATE TABLE lumen.window_counts_queue
(
    window_start DateTime64(3, 'UTC'), window_end DateTime64(3, 'UTC'), event_type String,
    events UInt64, amount_cents Int64, sessions UInt64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'agg.windows',
         kafka_group_name = 'clickhouse-windows',
         kafka_format = 'JSONEachRow',
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW lumen.window_counts_mv TO lumen.window_counts AS
SELECT *, now64(3, 'UTC') AS _ingested_at FROM lumen.window_counts_queue;

-- One row per closed session: how far down the funnel it got.
CREATE TABLE lumen.sessions
(
    session_id        String,
    session_start     DateTime64(3, 'UTC'),
    session_end       DateTime64(3, 'UTC'),
    customer_id       Nullable(String),
    client            LowCardinality(String),
    events            UInt32,
    page_views        UInt32,
    reached_cart      UInt8,
    reached_checkout  UInt8,
    converted         UInt8,
    revenue_cents     Int64,
    _ingested_at      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (session_id, session_start);

CREATE TABLE lumen.sessions_queue
(
    session_id String, session_start DateTime64(3, 'UTC'), session_end DateTime64(3, 'UTC'),
    customer_id Nullable(String), client String, events UInt32, page_views UInt32,
    reached_cart UInt8, reached_checkout UInt8, converted UInt8, revenue_cents Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'agg.sessions',
         kafka_group_name = 'clickhouse-sessions',
         kafka_format = 'JSONEachRow',
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW lumen.sessions_mv TO lumen.sessions AS
SELECT *, now64(3, 'UTC') AS _ingested_at FROM lumen.sessions_queue;

-- Events that arrived after the watermark had passed them: kept, never silently dropped.
CREATE TABLE lumen.late_events
(
    event_id            String,
    event_type          LowCardinality(String),
    event_time          DateTime64(3, 'UTC'),
    watermark_at_arrival DateTime64(3, 'UTC'),
    _ingested_at        DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY event_id;

CREATE TABLE lumen.late_events_queue
(
    event_id String, event_type String, event_time DateTime64(3, 'UTC'),
    watermark_at_arrival DateTime64(3, 'UTC')
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'events.late',
         kafka_group_name = 'clickhouse-late',
         kafka_format = 'JSONEachRow',
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW lumen.late_events_mv TO lumen.late_events AS
SELECT *, now64(3, 'UTC') AS _ingested_at FROM lumen.late_events_queue;

-- The dead-letter topic, byte for byte, with the gatekeeper's reason.
CREATE TABLE lumen.dlq
(
    key           String,
    reason        LowCardinality(String),
    source_partition UInt16,
    source_offset UInt64,
    payload       String,
    _ingested_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (source_partition, source_offset);

CREATE TABLE lumen.dlq_queue (payload String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'events.dlq',
         kafka_group_name = 'clickhouse-dlq',
         kafka_format = 'RawBLOB',
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW lumen.dlq_mv TO lumen.dlq AS
SELECT
    _key AS key,
    _headers.value[indexOf(_headers.name, 'error.reason')] AS reason,
    toUInt16(_headers.value[indexOf(_headers.name, 'source.partition')]) AS source_partition,
    toUInt64(_headers.value[indexOf(_headers.name, 'source.offset')]) AS source_offset,
    payload,
    now64(3, 'UTC') AS _ingested_at
FROM lumen.dlq_queue;
