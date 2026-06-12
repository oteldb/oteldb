# odbsafetyprocessor

`odbsafetyprocessor` is an OpenTelemetry Collector logs processor that applies oteldb log safety limiting to pdata logs.

## Registration

Use `odbsafetyprocessor.NewFactory()` in a custom collector factory set. The oteldb collector factory map already registers it for oteldb builds.

## Configuration

```yaml
processors:
  odbsafety:
    workload: my-service
    namespace: my-team
    max_rate_per_second: 10000
    on_excess: compact
    sample_rate: 0.01
    compact_window: 30s
    compact_threshold: 100
    compact_max_buckets: 10000
    compact_key_fields: [body]
    truncate_threshold: 1000
    redact_fields: [password, token]
```

Supported `on_excess` modes are `consume`, `drop`, `sample`, `compact`, and `truncate`.

For receiver-level protection before pdata batching, use the stanza `odbsafety` operator in `filelog.operators`.
