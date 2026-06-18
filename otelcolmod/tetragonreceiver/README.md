# Tetragon Receiver

| Status                   |                 |
| ------------------------ | --------------- |
| Stability                | [inDevelopment] |
| Supported pipeline types | logs            |
| Distributions            | [otelcol-contrib] |

`tetragonreceiver` streams events from [Tetragon](https://tetragon.io/) via gRPC and emits OTLP Logs into the collector pipeline. It consumes the `GetEvents` gRPC stream provided by the Tetragon agent — one OTLP log record per Tetragon event.

## Prerequisites

- Tetragon deployed and running in your Kubernetes cluster.
- Tetragon gRPC API reachable from the collector (default port `54321`).

### Deploy Tetragon

The quickest way to install Tetragon on Kubernetes:

```bash
helm repo add cilium https://helm.cilium.io
helm install tetragon cilium/tetragon -n kube-system --create-namespace
```

Or follow the [official Kubernetes install guide](https://tetragon.io/docs/getting-started/install-k8s/).

After deployment, verify the agent is running:

```bash
kubectl -n kube-system get pods -l app.kubernetes.io/name=tetragon
```

### Service address

By default Tetragon exposes the gRPC API on port `54321`. If deploying via Helm, you can expose it as a ClusterIP Service:

```bash
kubectl -n kube-system expose pod -l app.kubernetes.io/name=tetragon \
  --port=54321 --target-port=54321 --name=tetragon-api
```

For development, port-forward to localhost:

```bash
kubectl -n kube-system port-forward service/tetragon-api 54321:54321
```

## Configuration

```yaml
receivers:
  tetragonreceiver:
    # gRPC endpoint of the Tetragon agent.
    endpoint: tetragon-api.kube-system.svc.cluster.local:54321

    # TLS configuration. mTLS is recommended in production —
    # Tetragon exposes kernel-level security events.
    tls:
      insecure: true  # dev/test only

    # ClusterID is a numeric cluster identifier set on every log record as
    # "tetragon.cluster.id".  (optional)
    cluster_id: 12345
```

> `ClusterName` does not need to be configured — it is read directly from the
> `GetEventsResponse` stream.

### Config fields

| Field         | Type   | Default | Description                                      |
| ------------- | ------ | ------- | ------------------------------------------------ |
| `endpoint`    | string | `""`    | gRPC target of Tetragon agent **(required)**     |
| `tls`         | object | —       | TLS settings (see [configgrpc])                  |
| `cluster_id`  | int    | `0`     | Numeric cluster identifier (0 = omitted)         |

[configgrpc]: https://github.com/open-telemetry/opentelemetry-collector/tree/main/config/configgrpc

## Pipeline example

```yaml
receivers:
  tetragonreceiver:
    endpoint: tetragon-api.kube-system.svc.cluster.local:54321
    tls:
      insecure: true
    cluster_id: 12345

processors:
  batch:
    send_batch_size: 10000
    timeout: 300ms

exporters:
  otlp:
    endpoint: oteldb:4317

service:
  pipelines:
    logs:
      receivers: [tetragonreceiver]
      processors: [batch]
      exporters: [otlp]
```

## Translator behaviour

Each Tetragon `GetEventsResponse` becomes one OTLP log record. The process's pod is
mapped to resource attributes (`k8s.namespace.name`, `k8s.pod.name`, `k8s.cluster.name`),
and the event payload is flattened into log record attributes.

### Supported event types

| Event type              | `event.name` attribute      | Severity |
| ----------------------- | --------------------------- | -------- |
| ProcessExec             | `process_exec`              | INFO     |
| ProcessExit             | `process_exit`              | INFO     |
| ProcessKprobe           | `process_kprobe`            | DEBUG    |
| ProcessTracepoint       | `process_tracepoint`        | DEBUG    |
| ProcessLoader           | `process_loader`            | INFO     |
| Unknown / other         | — (dropped, `parse_errors`) | —        |

### Key attributes

| Attribute                          | Source                          |
| ---------------------------------- | ------------------------------- |
| `event.name`                       | Event type name                 |
| `process.pid`                      | `Process.Pid`                   |
| `process.executable.path`          | `Process.Binary`                |
| `process.command_args`             | `Process.Arguments`             |
| `process.owner.id`                 | `Process.Uid`                   |
| `tetragon.process.exec_id`         | `Process.ExecId`                |
| `tetragon.process.cwd`             | `Process.Cwd`                   |
| `tetragon.process.flags`           | `Process.Flags`                 |
| `tetragon.process.docker`          | `Process.Docker`                |
| `tetragon.process.start_time`      | `Process.StartTime` (RFC3339Nano) |
| `tetragon.parent.process.*`        | Parent process (same fields)    |
| `tetragon.node_name`               | Response-level node name        |
| `tetragon.kprobe.function_name`    | Kprobe function name            |
| `tetragon.ancestors_json`          | ProcessExec ancestors (JSON)    |
| `k8s.container.name`               | Pod container name              |
| `container.image.id`               | Pod container image ID          |

## TLS

In production, configure mTLS between the receiver and the Tetragon agent. Refer to
[Tetragon gRPC TLS documentation](https://tetragon.io/docs/installation/grpc-tls/).