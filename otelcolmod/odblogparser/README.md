[odbagent]: ../../cmd/odbagent

# odblogparser

| Status                   |                      |
| ------------------------ | -------------------- |
| Stability                | [inDevelopment]      |
| Supported pipeline types | logs (stanza)        |
| Distributions            | [odbagent]    |

`odblogparser` is a [stanza](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/stanza) parser operator for custom OpenTelemetry Collector builds. It adapts oteldb's internal log parsers to the collector-contrib stanza pipeline, supporting `generic-json`, `logfmt`, `zap-development`, and `klog` formats.

## Registration

Import the package for side-effect registration in a custom collector distribution:

```go
import _ "github.com/oteldb/oteldb/otelcolmod/odblogparser"
```

For an explicit registry:

```go
import "github.com/oteldb/oteldb/otelcolmod/odblogparser"

odblogparser.Register(registry)
```

## Configuration

```yaml
receivers:
  filelog:
    include: [/var/log/app/*.log]
    operators:
      - type: odblogparser
        parse_from: body
        parse_to: attributes
        detect: true
        detect_formats: [generic-json, logfmt, zap-development, klog]
```

### Config fields

| Field            | Type     | Default                | Description                                         |
| ---------------- | -------- | ---------------------- | --------------------------------------------------- |
| `parse_from`     | string   | inherited from helper  | Stanza field to parse                                |
| `parse_to`       | string   | inherited from helper  | Target attribute field                               |
| `format`         | string   | `""`                   | Force a single parser (`generic-json`, `logfmt`, …)  |
| `detect`         | bool     | `true`                 | Auto-detect format when `format` is unset            |
| `detect_formats` | []string | `["generic-json"]`     | Ordered list of formats to try during detection      |

### Supported formats

| Format              | Description                                             |
| ------------------- | ------------------------------------------------------- |
| `generic-json`      | Generic JSON-structured logs                             |
| `logfmt`            | Key=value logfmt format (rsyslog, go-kit, logrus)       |
| `zap-development`   | Uber zap's development-friendly text format              |
| `klog`              | Kubernetes klog format                                   |

Use `format` to force a single parser and skip detection:

```yaml
operators:
  - type: odblogparser
    format: generic-json
```

The operator writes parsed attributes to `parse_to`, updates stanza entry fields for timestamp, severity, trace ID, span ID, resource attributes, and leaves the parsed message body in `body`.
