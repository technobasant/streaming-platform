-- Job 2 — windows: event-time analytics over events.clean.
--   * 10-second tumbling counts per event type          -> agg.windows
--   * sessions (30 s inactivity gap) with funnel stage   -> agg.sessions
--   * every event already behind the watermark           -> events.late
-- Windows drop late records; the third statement is what keeps them from vanishing.

SET 'pipeline.name' = 'windows';
SET 'parallelism.default' = '2';
-- Window and session state is small and short-lived: keep it on the heap. With
-- RocksDB, the cluster default, this job topped out near 2,800 events/s.
SET 'state.backend.type' = 'hashmap';
-- A partition with no traffic must not hold the watermark back for ever. But a
-- partition that is only stalled must not be declared idle either: the watermark
-- would move on without it, and everything it then delivers is late. Killing a
-- broker stalled some partitions for up to 18 s, and with a 10 s timeout 1.7% of
-- that run's on-time events went to events.late. 60 s outlasts a leader election
-- here; the cost is that windows close up to a minute late when traffic stops.
SET 'table.exec.source.idle-timeout' = '60 s';

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
  is_probe     BOOLEAN,
  -- Out-of-order tolerance: an event up to 5 s behind the newest one seen is on time.
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'events.clean',
  'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
  'properties.group.id' = 'flink-windows',
  'properties.isolation.level' = 'read_committed',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE agg_windows (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  event_type   STRING,
  events       BIGINT,
  amount_cents BIGINT,
  sessions     BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'agg.windows',
  'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'SQL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'sp-windows',
  'properties.transaction.timeout.ms' = '600000'
);

CREATE TABLE agg_sessions (
  session_id       STRING,
  session_start    TIMESTAMP(3),
  session_end      TIMESTAMP(3),
  customer_id      STRING,
  client           STRING,
  events           BIGINT,
  page_views       BIGINT,
  reached_cart     INT,
  reached_checkout INT,
  converted        INT,
  revenue_cents    BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'agg.sessions',
  'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'SQL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'sp-sessions',
  'properties.transaction.timeout.ms' = '600000'
);

CREATE TABLE events_late (
  event_id             STRING,
  event_type           STRING,
  event_time           TIMESTAMP(3),
  watermark_at_arrival TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'events.late',
  'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
  'key.format' = 'raw',
  'key.fields' = 'event_id',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'SQL',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'sp-late',
  'properties.transaction.timeout.ms' = '600000'
);

CREATE TEMPORARY VIEW shopper_events AS
SELECT * FROM events_clean WHERE NOT is_probe;

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO agg_windows
  SELECT window_start, window_end, event_type,
         COUNT(*), SUM(COALESCE(amount_cents, 0)), COUNT(DISTINCT session_id)
  FROM TABLE(TUMBLE(TABLE shopper_events, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
  GROUP BY window_start, window_end, event_type;

  INSERT INTO agg_sessions
  SELECT session_id, window_start, window_end,
         MAX(customer_id), MAX(client),
         COUNT(*),
         SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END),
         MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END),
         MAX(CASE WHEN event_type = 'checkout_started' THEN 1 ELSE 0 END),
         MAX(CASE WHEN event_type = 'payment_succeeded' THEN 1 ELSE 0 END),
         SUM(CASE WHEN event_type = 'payment_succeeded' THEN amount_cents ELSE 0 END)
  FROM TABLE(SESSION(TABLE shopper_events PARTITION BY session_id,
                     DESCRIPTOR(event_time), INTERVAL '30' SECOND))
  GROUP BY session_id, window_start, window_end;

  -- The window operator drops a record when the watermark has reached the last
  -- millisecond of its window (window_end - 1 ms), not merely its own timestamp.
  -- Route on the same condition so that every event lands in exactly one place.
  INSERT INTO events_late
  SELECT event_id, event_type, event_time, CURRENT_WATERMARK(event_time)
  FROM shopper_events
  WHERE CURRENT_WATERMARK(event_time) IS NOT NULL
    AND TIMESTAMPADD(SECOND, 10 - MOD(CAST(EXTRACT(SECOND FROM event_time) AS INT), 10),
          CAST(FLOOR(event_time TO SECOND) AS TIMESTAMP(3))) - INTERVAL '0.001' SECOND
        <= CURRENT_WATERMARK(event_time);
END;
