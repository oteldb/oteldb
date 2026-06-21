[odbagent]: ../../cmd/odbagent

# chreceiver

| Status                   |                 |
| ------------------------ | --------------- |
| Stability                | [inDevelopment] |
| Supported pipeline types | traces          |
| Distributions            | [odbagent] |

`chreceiver` reads ClickHouse internal spans from `system.opentelemetry_span_log` and emits OTLP traces into the collector pipeline. The reader uses a cursor on `finish_time_us`, so the span log should be ordered by finish time.

## Prerequisites

- ClickHouse server with `opentelemetry_span_log` enabled.
- A ClickHouse user with `SELECT` on `system.opentelemetry_span_log`.

## ClickHouse configuration

Add the `opentelemetry_span_log` section to `clickhouse.xml`:

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

### Table schema

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
        'INTERNAL' = 0, 'SERVER' = 1, 'CLIENT' = 2,
        'PRODUCER' = 3, 'CONSUMER' = 4
    )
)
ENGINE = MergeTree
ORDER BY (finish_date, finish_time_us, trace_id)
TTL toDateTime(finish_time_us / 1000000) + toIntervalMinute(15);
```

## Configuration

```yaml
receivers:
  chreceiver:
    # ClickHouse DSN.  (required)
    dsn: clickhouse://default:@localhost:9000/default

    # How often to poll the span log table.
    poll_rate: 500ms

    # How far behind "now" to start reading. Guards against incomplete writes.
    lag: 30s

    # How far back to scan on first run.
    lookback: 5m

    # Span filtering (optional).
    filter:
      exclude:
        - '*TCPHandler::sendData*'
      include: []
      collapse: true
```

### Config fields

| Field            | Type     | Default                                                  | Description                                              |
| ---------------- | -------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `dsn`            | string   | `"clickhouse://default:@localhost:9000/default"`         | ClickHouse DSN **(required)**                            |
| `poll_rate`      | duration | `500ms`                                                  | Polling interval                                          |
| `lag`            | duration | `30s` (`chotel.DefaultLag`)                              | Lag behind wall clock                                     |
| `lookback`       | duration | `5m` (`chotel.DefaultLookback`)                          | Initial lookback window                                   |
| `filter.exclude` | []string | `[]`                                                     | Glob patterns to exclude                                  |
| `filter.include` | []string | `[]`                                                     | Glob patterns to keep (empty = all)                       |
| `filter.collapse`| bool     | `false`                                                  | Merge same-name spans within a trace                      |

## Pipeline example

```yaml
receivers:
  chreceiver:
    dsn: clickhouse://default:secret@clickhouse:9000/otel
    poll_rate: 500ms
    lag: 30s
    lookback: 5m

processors:
  batch:
    send_batch_size: 10000
    timeout: 300ms

exporters:
  otlp:
    endpoint: oteldb:4317

service:
  pipelines:
    traces:
      receivers: [chreceiver]
      processors: [batch]
      exporters: [otlp]
```
