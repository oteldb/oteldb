<p align="center">
<img height="256" src="logo.svg" alt="oteldb svg logo">
</p>

# oteldb [![codecov](https://img.shields.io/codecov/c/github/oteldb/oteldb?label=cover)](https://codecov.io/gh/oteldb/oteldb) [![experimental](https://img.shields.io/badge/-experimental-blueviolet)](https://go-faster.org/docs/projects/status#experimental)

The next generation, [OpenTelemetry-first][otel] aggregation system for metrics, traces and logs.

Compatible with [PromQL][promql], [TraceQL][traceql] and [LogQL][logql].

Based on [ClickHouse][clickhouse], fastest open-source (Apache 2.0) column-oriented database.

[clickhouse]: https://clickhouse.com/
[otel]: https://opentelemetry.io/

Supported query languages:
- [PromQL][promql] ([Prometheus][prometheus]) for metrics, [>99% compatibility][compliance]
- [TraceQL][traceql] ([Grafana Tempo][tempo]) for traces
- [LogQL][logql] ([Grafana Loki][loki]) for logs

[traceql]: https://grafana.com/docs/tempo/latest/traceql/
[logql]: https://grafana.com/docs/loki/latest/query/
[promql]: https://prometheus.io/docs/prometheus/latest/querying/basics/

[prometheus]: https://prometheus.io/
[loki]: https://grafana.com/oss/loki/
[tempo]: https://grafana.com/oss/tempo/

Supported ingestion protocols:
- Prometheus remote write, including [exemplars][exemplars]
- OpenTelemetry protocol (gRPC) for metrics, traces and logs

Ingestion is possible with [OpenTelemetry collector][otelcol], supporting [over 90 protocols][otelcol-contrib].

[otelcol]: https://opentelemetry.io/docs/collector/
[otelcol-contrib]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver
[exemplars]: https://grafana.com/docs/grafana/latest/fundamentals/exemplars/

## Demo

https://github.com/user-attachments/assets/647d220c-052a-40d4-9358-7d6039a0f198

## Prometheus Compatibility

See [ch-compliance][compliance] for Prometheus compatibility tests.

```console
$ promql-compliance-tester -config-file promql-test-queries.yml -config-file test.oteldb.yml
Total: 547 / 548 (99.82%) passed, 0 unsupported
```

[compliance]: ./dev/local/ch-compliance

## Quick Start

Setup oteldb, ClickHouse, Grafana, and telemetry generators:

```shell
docker compose -f dev/local/ch/docker-compose.yml up -d
```

You can open Grafana dashboard at http://localhost:3000/d/oteldb/oteldb

## Embedded storage

oteldb can also run on the embedded [storage engine][storage] instead of ClickHouse. The engine is
in-process and requires no external dependencies, which makes it convenient for local development,
testing, and small single-node deployments. It serves all signals: metrics (PromQL), traces
(TraceQL), logs (LogQL) and profiles (Pyroscope). PromQL on the embedded engine passes the full
[ch-compliance][compliance] suite (537/537, 100%).

[storage]: https://github.com/oteldb/storage

Enable it for every signal with a single flag:

```shell
oteldb --embedded
```

This runs oteldb as a fully self-contained binary: ClickHouse is not started at all (not even the
zero-config embedded ClickHouse) and no DSN is required. It is shorthand for setting each signal's
backend to `storage` in the config. You can also enable
it per signal, and switch the engine from the default ephemeral in-memory store to an on-disk one:

```yaml
# oteldb.yml
metrics_backend: storage
traces_backend: storage
logs_backend: storage
profiles_backend: storage

storage:
  backend: file       # "memory" (default, ephemeral) or "file"
  dir: ./oteldb-data  # data directory for the file backend
  flush_interval: 1m  # max age of unflushed head data before it is flushed to a part
```

Any signal left unset (or set to `clickhouse`) keeps using ClickHouse, so the two backends can be
mixed. Profiles have no ClickHouse implementation and are served only when `profiles_backend` is
`storage`.

## Profiling oteldb itself

Both are off by default, need no code or config-file changes, and are driven entirely by the
environment (`go-faster/sdk` wires them from `app.Run`).

**Continuous profiling** ships CPU, heap, goroutine, mutex and block profiles to any
Pyroscope-compatible endpoint — including another oteldb, since oteldb serves the Pyroscope ingest
API itself. Enabling it also links traces to profiles, so a span can be opened straight into the
profile recorded while it ran:

```shell
PYROSCOPE_ENABLE=true                  # required; anything strconv.ParseBool accepts
PYROSCOPE_URL=http://oteldb-svc:4040   # ingest endpoint
PYROSCOPE_APP_NAME=oteldb              # application name the profiles appear under
PYROSCOPE_USER=...                     # optional basic auth
PYROSCOPE_PASSWORD=...                 # optional basic auth
PYROSCOPE_TENANT_ID=...                # optional multi-tenant header
```

Mutex and block profiling are sampled by the Go runtime rather than collected on demand; enabling
Pyroscope switches both on (`SetMutexProfileFraction(5)`, `SetBlockProfileRate(5)`), which costs a
little throughput on contended locks.

This is what catches a slow leak: `inuse_space` growing across hours is invisible to a profile
taken by hand, because by the time anyone looks at the memory graph the process is minutes from an
OOM kill.

**On-demand pprof** exposes the standard `/debug/pprof/` handlers on their own listener:

```shell
PPROF_ADDR=:6060
```

Note that the two can interfere: while the Pyroscope client is collecting, an overlapping CPU
profile fetched from `/debug/pprof/profile` may fail.

## License

Apache License 2.0, see [LICENSE](./LICENSE).
