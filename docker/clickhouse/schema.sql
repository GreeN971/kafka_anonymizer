--Base table
CREATE TABLE IF NOT EXISTS logs_kafka
(
  timestampEpochMilli UInt64,
  resourceId          UInt64,
  bytesSent           UInt64,
  requestTimeMilli    UInt64,
  responseStatus      UInt16,
  cacheStatus         String,
  method              String,
  remoteAddr          String,
  url                 String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'broker:29092', -- Client requests to be routed over the Docker network to the broker service, use port 29092
  kafka_topic_list = 'http_logs_test',
  kafka_group_name = 'probe',
  kafka_format = 'CapnProto',
  kafka_schema = 'http_log.capnp:HttpLogRecord', -- file must be in format_schema_path on server 
  kafka_poll_timeout_ms = 60000; 
CREATE TABLE IF NOT EXISTS logs_raw
(
  ts DateTime64(3),
  resourceId UInt64,
  bytesSent UInt64,
  reqTimeMs UInt64,
  status UInt16,
  cacheStatus LowCardinality(String),
  method LowCardinality(String),
  remoteIP String,
  url String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, resourceId, status);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_logs_raw
TO logs_raw
AS
SELECT
  toDateTime64(timestampEpochMilli / 1000.0, 3) AS ts,
  resourceId,
  bytesSent,
  requestTimeMilli AS reqTimeMs,
  responseStatus AS status,
  cacheStatus,
  method,
  remoteAddr AS remoteIP,  --both v4 and v6
  url
FROM logs_kafka;

--Bytes sent global
CREATE TABLE IF NOT EXISTS bytes_per_min
(
    `m` DateTime,
    `raw_sum` UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(m)
ORDER BY m;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_raw_to_bytes_per_min
TO bytes_per_min
AS
SELECT
  toStartOfMinute(ts) AS `m`,
  bytesSent AS `raw_sum`
FROM logs_raw
ORDER BY m;

--Bytes Sent per Region
CREATE TABLE IF NOT EXISTS bytes_per_min_localized
(
  ts DateTime,
  sourceId UInt64,     
  bytes    UInt64      
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, sourceId);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_bytes_per_min_localized
TO bytes_per_min_localized
AS SELECT
    toStartOfMinute(ts) AS ts,
    resourceId AS sourceId,
    sum(bytesSent) AS bytes
FROM logs_raw
GROUP BY (ts, sourceId);

--Response Status
CREATE TABLE IF NOT EXISTS status_counts
(
    ts_minute DateTime,
    status UInt16,
    cnt UInt64
)
ENGINE = AggregatingMergeTree --maybe summing
PARTITION BY toYYYYMM(ts_minute)
ORDER BY (ts_minute, status);

CREATE MATERIALIZED VIEW  IF NOT EXISTS mv_status_counts 
TO status_counts
AS SELECT
    toStartOfMinute(toDateTime(timestampEpochMilli / 1000)) AS ts_minute,
    toString(responseStatus) AS status,
    count() AS cnt
FROM logs_kafka
GROUP BY (ts_minute, status);

--Cache counts
CREATE TABLE IF NOT EXISTS cache_counts
(
    ts_minute DateTime,
    cacheStatus LowCardinality(String),
    cnt UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(ts_minute)
ORDER BY (ts_minute, cacheStatus);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cache_counts 
TO cache_counts
AS SELECT
    toStartOfMinute(toDateTime(timestampEpochMilli / 1000)) AS ts_minute,
    toString(cacheStatus) AS cacheStatus,
    count() AS cnt
FROM logs_kafka
GROUP BY (ts_minute, cacheStatus);

