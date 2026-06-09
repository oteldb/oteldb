# odblogparser

`odblogparser` is a stanza parser operator for custom OpenTelemetry Collector builds. It adapts oteldb's internal log parsers to the collector-contrib stanza pipeline.

## Registration

Import the package for side-effect registration in a custom collector distribution:

```go
import _ "github.com/oteldb/oteldb/odbagent/odblogparser"
```

For an explicit registry, call `odblogparser.Register(registry)`.

## Configuration

```yaml
receivers:
  filelog:
    include: [/var/log/app/*.log]
    operators:
      - type: odblogparser
        parse_from: body
        parse_to: attributes
        detect_formats: [generic-json, logfmt, zap-development, klog]
```

Use `format` to force a single parser and skip detection:

```yaml
operators:
  - type: odblogparser
    format: generic-json
```

The operator writes parsed attributes to `parse_to`, updates stanza entry fields for timestamp, severity, trace ID, span ID, resource attributes, and leaves the parsed message body in `body`.

Supported parser names come from `internal/logparser`: `generic-json`, `logfmt`, `zap-development`, and `klog`.
