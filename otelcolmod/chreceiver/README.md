# chreceiver

`chreceiver` reads ClickHouse internal spans from `system.opentelemetry_span_log` and emits OTLP traces into the collector pipeline.

The reader uses a cursor on `finish_time_us`, so the span log should be ordered by finish time.

## ClickHouse Config

Example `clickhouse.xml` snippet:

```xml
<clickhouse>
    <opentelemetry_span_log>
        <engine>
            engine MergeTree
            order by (finish_date, finish_time_us, trace_id)
            ttl toDateTime(finish_time_us / 1000000) + toIntervalMinute(15)
        </engine>
        <database>system</database>
        <table>opentelemetry_span_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </opentelemetry_span_log>
</clickhouse>
```

## Table Shape

ClickHouse creates the system table from the `opentelemetry_span_log` config. The receiver expects these columns:

```sql
CREATE TABLE system.opentelemetry_span_log
(
    trace_id UUID,
    span_id UInt64,
    parent_span_id UInt64,
    operation_name LowCardinality(String),
    start_time_us UInt64,
    finish_time_us UInt64,
    finish_date Date,
    attribute Map(LowCardinality(String), String),
    kind Enum8(
        'INTERNAL' = 0,
        'SERVER' = 1,
        'CLIENT' = 2,
        'PRODUCER' = 3,
        'CONSUMER' = 4
    )
)
ENGINE = MergeTree
ORDER BY (finish_date, finish_time_us, trace_id)
TTL toDateTime(finish_time_us / 1000000) + toIntervalMinute(15);
```

## Receiver Config

```yaml
receivers:
  chreceiver:
    dsn: clickhouse://default:@localhost:9000/default
    poll_rate: 500ms
    lag: 30s
    lookback: 5m
    filter:
      exclude:
        - '*TCPHandler::sendData*'
      collapse: true
```
