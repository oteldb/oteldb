[odbagent]: ../../cmd/odbagent

# odbsafety

| Status                   |                      |
| ------------------------ | -------------------- |
| Stability                | [inDevelopment]      |
| Supported pipeline types | logs (stanza)        |
| Distributions            | [odbagent]    |

`odbsafety` is a [stanza](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/stanza) transformer operator for custom OpenTelemetry Collector builds. It applies oteldb log safety limiting inside receivers such as `filelog`, before entries are converted into pdata batches.

Use this operator early in stanza pipelines to avoid buffering high-volume spam in collector pdata batches. Use `odbsafetyprocessor` when logs are already in pdata form.

## Registration

Import the package for side-effect registration in a custom collector distribution:

```go
import _ "github.com/oteldb/oteldb/otelcolmod/odbsafety"
```

For an explicit registry:

```go
import "github.com/oteldb/oteldb/otelcolmod/odbsafety"

odbsafety.Register(registry)
```

## Configuration

```yaml
receivers:
  filelog:
    include: [/var/log/app/*.log]
    operators:
      - type: odbsafety
        # Soft throttle — 0 means no limit. Above this, `on_excess` engages.
        soft_max_rate_per_second: 5000
        # Hard throttle — 0 means no limit. Above this, `hard_on_excess` engages,
        # escalating past the soft limit.
        hard_max_rate_per_second: 10000
        # What to do when the soft rate is exceeded.
        on_excess: compact
        # What to do when the hard rate is exceeded.
        hard_on_excess: drop
        # First N entries logged unconditionally for 'sample' mode, then
        # 1-in-sample_thereafter after that.
        sample_first: 100
        sample_thereafter: 100
        # Compaction window and thresholds.
        compact_window: 30s
        compact_threshold: 100
        compact_max_buckets: 10000
        compact_key_fields: [body]
        # Min suppressed entries in a window before emitting a synthetic
        # truncation record for 'truncate' mode (0 = always emit).
        truncate_threshold: 1000
        # Fields whose values are redacted.
        redact_fields: [password, token]
```

### Config fields

| Field                      | Type     | Default     | Description                                                          |
| -------------------------- | -------- | ----------- | --------------------------------------------------------------------- |
| `soft_max_rate_per_second` | int      | `0`         | Entries per second above which `on_excess` engages (`0` = unlimited)  |
| `hard_max_rate_per_second` | int      | `0`         | Entries per second above which `hard_on_excess` engages (`0` = unlimited) |
| `on_excess`                | string   | `"consume"` | Action when the soft rate is exceeded (see below)                     |
| `hard_on_excess`           | string   | `"drop"`    | Action when the hard rate is exceeded (see below)                     |
| `sample_first`              | int      | `100`       | Entries logged unconditionally before sampling kicks in, for `sample` mode |
| `sample_thereafter`         | int      | `100`       | Sample 1-in-N entries once `sample_first` is exceeded, for `sample` mode |
| `compact_window`           | duration | `30s`       | Time window for compaction/truncation buckets                         |
| `compact_threshold`        | int      | `100`       | Min identical entries before compaction                               |
| `compact_max_buckets`      | int      | `10000`     | Max compaction buckets                                                |
| `compact_key_fields`       | []string | `[]`        | Fields used to group entries for compaction (default: body)           |
| `truncate_threshold`       | int      | `0`         | Min suppressed entries in a window before emitting a truncation record |
| `redact_fields`            | []string | `[]`        | Field names whose values are replaced with `<redacted>`                |

Entries with the `oteldb.passthrough` attribute set to `true` bypass excess handling entirely.

### Single-tier rate limiting and probabilistic sampling

The old config had a single `max_rate_per_second` threshold and a probabilistic
`sample_rate`. To get the same behaviour with the current fields, leave the
hard tier disabled and use `sample_thereafter` alone — sampling 1-in-N
approximates a `1/N` probabilistic rate without needing randomness:

```yaml
      - type: odbsafety
        # Single threshold, same as the old max_rate_per_second.
        soft_max_rate_per_second: 10000
        hard_max_rate_per_second: 0 # disabled — no escalation tier
        on_excess: sample
        # ~1% sampling, like the old sample_rate: 0.01.
        sample_first: 0
        sample_thereafter: 100
```

### On-excess modes

| Mode       | Behaviour                                                          |
| ---------- | -------------------------------------------------------------------- |
| `consume`  | Pass through (no limit)                                              |
| `drop`     | Silently drop entries above the rate limit                           |
| `sample`   | Log the first `sample_first` entries, then 1-in-`sample_thereafter`  |
| `compact`  | Merge identical entries within the compaction window                 |
| `truncate` | Emit a synthetic "N suppressed" record per window instead of entries |
