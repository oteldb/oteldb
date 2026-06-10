# odbsafety

`odbsafety` is a stanza transformer operator for custom OpenTelemetry Collector builds. It applies oteldb log safety limiting inside receivers such as `filelog`, before entries are converted into pdata batches.

## Registration

Import the package for side-effect registration in a custom collector distribution:

```go
import _ "github.com/oteldb/oteldb/odbsafety"
```

For an explicit registry, call `odbsafety.Register(registry)`.

## Configuration

```yaml
receivers:
  filelog:
    include: [/var/log/app/*.log]
    operators:
      - type: odbsafety
        max_rate_per_second: 10000
        on_excess: compact
        compact_window: 30s
        compact_threshold: 100
        compact_max_buckets: 10000
        compact_key_fields: [body]
        truncate_threshold: 1000
        redact_fields: [password, token]
```

Supported `on_excess` modes are `consume`, `drop`, `sample`, `compact`, and `truncate`.

Use this operator early in stanza pipelines to avoid buffering high-volume spam in collector pdata batches. Use `odbsafetyprocessor` when logs are already in pdata form.
