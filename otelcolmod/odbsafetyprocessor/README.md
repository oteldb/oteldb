[odbagent]: ../../cmd/odbagent

# odbsafetyprocessor

| Status                   |                 |
| ------------------------ | --------------- |
| Stability                | [inDevelopment] |
| Supported pipeline types | logs            |
| Distributions            | [odbagent] |

`odbsafetyprocessor` is an OpenTelemetry Collector logs processor that applies oteldb log safety limiting to pdata logs. It operates after logs have been batched into pdata, in contrast to the stanza `odbsafety` operator which protects at the receiver level.

## Registration

Use `odbsafetyprocessor.NewFactory()` in a custom collector factory set. The oteldb collector factory map already registers it for oteldb builds.

## Configuration

```yaml
processors:
  odbsafety:
    # Labels for rate limiting and metrics (optional).
    workload: my-service
    namespace: my-team

    # Throttle — 0 means no limit.
    max_rate_per_second: 10000
    # What to do when the rate is exceeded.
    on_excess: compact
    # Compaction window and thresholds.
    compact_window: 30s
    compact_threshold: 100
    compact_max_buckets: 10000
    compact_key_fields: [body]
    # Truncate long fields after N bytes (0 = no truncation).
    truncate_threshold: 1000
    # Fields whose values are redacted.
    redact_fields: [password, token]
    # Sampling rate for 'sample' mode.
    sample_rate: 0.01
```

### Config fields

| Field                | Type     | Default     | Description                                              |
| -------------------- | -------- | ----------- | -------------------------------------------------------- |
| `workload`           | string   | `""`        | Workload label for rate limiting (+)                     |
| `namespace`          | string   | `""`        | Namespace label for rate limiting (+)                     |
| `max_rate_per_second`| int      | `0`         | Max log entries per second (`0` = unlimited)             |
| `on_excess`          | string   | `"consume"` | Action when rate exceeded (see below)                    |
| `sample_rate`        | float64  | `0.01`      | Sampling fraction for `sample` mode                      |
| `compact_window`     | duration | `30s`       | Time window for compaction                               |
| `compact_threshold`  | int      | `100`       | Min identical entries before compaction                  |
| `compact_max_buckets`| int      | `10000`     | Max compaction buckets                                   |
| `compact_key_fields` | []string | `[]`        | Fields used to group entries for compaction              |
| `truncate_threshold` | int      | `0`         | Max bytes per field (`0` = no truncation)                |
| `redact_fields`      | []string | `[]`        | Field names whose values are replaced with `[REDACTED]`  |

### On-excess modes

| Mode       | Behaviour                                                  |
| ---------- | ---------------------------------------------------------- |
| `consume`  | Pass through (no limit)                                    |
| `drop`     | Silently drop entries above the rate limit                 |
| `sample`   | Probabilistically sample at `sample_rate`                  |
| `compact`  | Merge identical entries within the compaction window       |
| `truncate` | Truncate field values exceeding `truncate_threshold` bytes |

## Pipeline example

```yaml
receivers:
  filelog:
    include: [/var/log/app/*.log]

processors:
  odbsafety:
    workload: my-service
    namespace: my-team
    max_rate_per_second: 10000
    on_excess: compact

exporters:
  otlp:
    endpoint: oteldb:4317

service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [odbsafety]
      exporters: [otlp]
```
