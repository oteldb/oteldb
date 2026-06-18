# Hubble Receiver

| Status                   |                 |
| ------------------------ | --------------- |
| Stability                | [inDevelopment] |
| Supported pipeline types | logs            |
| Distributions            | [otelcol-contrib] |

`hubblereceiver` streams network flows from [Hubble Relay](https://docs.cilium.io/en/stable/observability/hubble/setup/) via gRPC and emits OTLP Logs into the collector pipeline. It is the gRPC-streaming equivalent of ingesting Cilium network flows — one OTLP log record per Hubble flow.

## Prerequisites

- A Kubernetes cluster running **Cilium v1.14+** with Hubble enabled.
- **Hubble Relay** deployed and reachable from the collector.

### Enable Hubble and deploy Relay

Hubble can be enabled with the Cilium CLI:

```bash
cilium hubble enable
```

Or via Helm:

```bash
helm upgrade cilium cilium/cilium --version v1.18.1 \
  --namespace kube-system \
  --reuse-values \
  --set hubble.relay.enabled=true
```

After deployment, verify Hubble Relay is running:

```bash
cilium status
```

For the full guide, see [Setting up Hubble Observability](https://docs.cilium.io/en/stable/observability/hubble/setup/).

### Service address

Hubble Relay exposes the gRPC observer API on port `80` via the `hubble-relay` Service in the `kube-system` namespace:

```
hubble-relay.kube-system.svc.cluster.local:80
```

For development or testing outside the cluster, port-forward and point the receiver at `localhost:4245`:

```bash
cilium hubble port-forward &
# or
kubectl -n kube-system port-forward service/hubble-relay 4245:80
```

## Configuration

```yaml
receivers:
  hubblereceiver:
    # gRPC endpoint of Hubble Relay.
    endpoint: hubble-relay.kube-system.svc.cluster.local:80

    # TLS configuration. mTLS is strongly recommended in production —
    # Hubble flows contain network topology data.
    tls:
      insecure: true  # dev/test only

    # ClusterID is a numeric cluster identifier set on every log record as
    # "hubble.cluster.id".  (optional)
    cluster_id: 12345

    # ClusterName is set as k8s.cluster.name on every log resource.  (optional)
    cluster_name: prod-cluster
```

### Config fields

| Field         | Type   | Default | Description                                      |
| ------------- | ------ | ------- | ------------------------------------------------ |
| `endpoint`    | string | `""`    | gRPC target of Hubble Relay **(required)**       |
| `tls`         | object | —       | TLS settings (see [configgrpc])                  |
| `cluster_id`  | int    | `0`     | Numeric cluster identifier (0 = omitted)         |
| `cluster_name`| string | `""`    | Cluster name set on resource attributes          |

[configgrpc]: https://github.com/open-telemetry/opentelemetry-collector/tree/main/config/configgrpc

## Pipeline example

```yaml
receivers:
  hubblereceiver:
    endpoint: hubble-relay.kube-system.svc.cluster.local:80
    tls:
      insecure: true
    cluster_id: 12345
    cluster_name: prod-cluster

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
      receivers: [hubblereceiver]
      processors: [batch]
      exporters: [otlp]
```

## Translator behaviour

Each Hubble `Flow` becomes one OTLP log record. The **source endpoint** is mapped to
resource attributes (`k8s.namespace.name`, `k8s.pod.name`), and the flow payload is
flattened into log record attributes.

### Verdict → severity mapping

| Hubble Verdict               | OTLP Severity |
| ---------------------------- | ------------- |
| `DROPPED`                    | `WARN`        |
| `ERROR`                      | `ERROR`       |
| `AUDIT`                      | `INFO`        |
| `REDIRECTED`, `TRACED`, `TRANSLATED` | `DEBUG` |
| `FORWARDED`, `VERDICT_UNKNOWN` | `INFO`      |

### Key attributes

| Attribute                        | Source                    |
| -------------------------------- | ------------------------- |
| `network.type`                   | IP version (ipv4 / ipv6)  |
| `network.transport`              | L4 protocol (tcp / udp / sctp / icmp / icmpv6) |
| `network.source.address`         | `IP.Source`               |
| `network.destination.address`    | `IP.Destination`          |
| `network.source.port`            | L4 source port            |
| `network.destination.port`       | L4 destination port       |
| `http.request.method`            | L7 HTTP method            |
| `url.full`                       | L7 HTTP URL               |
| `http.response.status_code`      | L7 HTTP status code       |
| `dns.question.name`              | L7 DNS query              |
| `trace_id`                       | Trace context propagation |

No inverse records are generated. Both source and destination are present as attributes
on one record — LogQL handles bidirectional search with `or` label matchers.

## TLS

In production, configure mTLS between the receiver and Hubble Relay. Refer to
[Hubble TLS documentation](https://docs.cilium.io/en/stable/observability/configuration/tls/).