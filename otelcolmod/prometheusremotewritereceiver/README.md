# Prometheus Remote Write Receiver

| Status                   |                 |
| ------------------------ | --------------- |
| Stability                | [inDevelopment] |
| Supported pipeline types | metrics         |
| Distributions            | [otelcol-contrib] |

`prometheusremotewritereceiver` ingests metrics via the Prometheus Remote Write protocol and emits OTLP metrics into the collector pipeline.

## Getting Started

Enable the receiver by including it in the receiver definitions:

```yaml
receivers:
  prometheusremotewrite:
    endpoint: 0.0.0.0:19291
```

### Config fields

| Field            | Type     | Default         | Description                                          |
| ---------------- | -------- | --------------- | ---------------------------------------------------- |
| `endpoint`       | string   | `"0.0.0.0:19291"` | Host:port to receive data                         |
| `tls`            | object   | —               | TLS settings (see [confighttp])                      |
| `cors`           | object   | —               | CORS settings                                        |
| `time_threshold` | duration | `24h`           | Drop timeseries older than this threshold            |

[confighttp]: https://github.com/open-telemetry/opentelemetry-collector/tree/main/config/confighttp

Additional HTTP server settings are documented at the
[confighttp server configuration](https://github.com/open-telemetry/opentelemetry-collector/tree/main/config/confighttp#server-configuration) reference.

## Pipeline example

```yaml
receivers:
  prometheusremotewrite:
    endpoint: 0.0.0.0:19291

processors:
  batch:
    send_batch_size: 10000
    timeout: 300ms

exporters:
  otlp:
    endpoint: oteldb:4317

service:
  pipelines:
    metrics:
      receivers: [prometheusremotewrite]
      processors: [batch]
      exporters: [otlp]
```