# All-signals embedded demo

A minimal, self-contained demo of **oteldb serving all four signals — logs, metrics, traces, and
profiles — from a single process** using the embedded storage engine (`--embedded`). No ClickHouse,
no external collector.

Profiles are the reason this demo uses the embedded engine: the profiles signal has no ClickHouse
implementation, so it is only available when oteldb runs on the embedded `github.com/oteldb/storage`
engine.

## Run

```bash
docker compose -f dev/local/embedded-demo/docker-compose.yml up --build
```

Then open Grafana at <http://localhost:3000> (anonymous admin, no login). Four datasources are
pre-provisioned, one per signal, all pointing at the single oteldb instance:

| Signal   | Datasource | Query language | oteldb port |
|----------|------------|----------------|-------------|
| Metrics  | PromQL     | PromQL         | 9090        |
| Logs     | LogQL      | LogQL          | 3100        |
| Traces   | TraceQL    | TraceQL        | 3200        |
| Profiles | Pyroscope  | ProfileQL      | 4040        |

Use **Explore** and pick a datasource. Try `service.name` `client` / `server` for traces and
profiles, `{service_name="server"}` in LogQL, and the `oteldemo.client.sent_requests` counter in
PromQL.

## How it works

```
client ─┐
server ─┼─ OTLP (logs/metrics/traces) ─▶ oteldb --embedded ─▶ Grafana (Loki/Tempo/Prometheus/Pyroscope)
profiles┘  OTLP (profiles) ────────────▶
```

- **oteldb** runs with `--embedded`, which routes every signal to the in-process storage engine and
  enables the experimental OTLP profiles pipeline. It exposes OTLP ingestion on `4317`/`4318` and the
  Loki, Tempo, Prometheus, and Pyroscope query APIs.
- **client** and **server** are the `oteldemo` binary; they exchange HTTP requests and export logs,
  metrics, and traces over OTLP via the OpenTelemetry Go SDK.
- **profiles** is the `oteldemo profiles` mode. The Go SDK has no stable profiles exporter, so it
  builds synthetic OTLP CPU profiles with the pdata API and pushes them to oteldb's OTLP endpoint
  once per second.

Storage is the ephemeral in-memory backend, so data does not survive a restart — the generators keep
a rolling window of recent data flowing.
