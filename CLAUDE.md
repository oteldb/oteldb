# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**oteldb** is an OpenTelemetry-first aggregation system for metrics, traces, and logs backed by ClickHouse. It provides PromQL, TraceQL, and LogQL query compatibility and ingests data via OTLP (gRPC) and Prometheus remote write.

Go module: `github.com/go-faster/oteldb`

Do not load generated files in directories:

 - `internal/lokiapi`
 - `internal/otelbotapi`
 - `internal/promapi`
 - `internal/promproxy`
 - `internal/pyroscopeapi`
 - `internal/sentryapi`
 - `internal/tempoapi`

## Commands

### MCP
- Prefer gopls MCP tools for symbol navigation, diagnostics, and references
- Use `go_diagnostics` before assuming a file has no errors
- Use `go_definition` instead of grep for finding symbol definitions

### Build
```bash
go build -o ./oteldb ./cmd/oteldb
```

### Test

#### Unit tests

```bash
# Run all tests with race detector
go test --timeout 10m -race ./...

# Run a single package's tests
go test ./internal/chstorage/... -run TestName

# Run with coverage
go test -failfast -race -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt ./... -timeout=5m
```

#### End-to-end tests

```bash
# Prometheus/PromQL
E2E='1' go test -timeout 1h -v github.com/go-faster/oteldb/integration/prome2e
# Loki/LogQL
E2E='1' go test -timeout 1h -v github.com/go-faster/oteldb/integration/lokie2e
# Tempo/TraceQL
E2E='1' go test -timeout 1h -v github.com/go-faster/oteldb/integration/tempoe2e
# chotel
E2E='1' go test -timeout 1h -v github.com/go-faster/oteldb/integration/chotele2e
```

### Lint
```bash
golangci-lint run ./...
```

### Format
```bash
golangci-lint fmt ./...
```

### Local dev environment (ClickHouse + Grafana + generators)
```bash
docker compose -f dev/local/ch/docker-compose.yml up -d
```

## Code Style

- **Error handling**: Use `github.com/go-faster/errors` (not stdlib `errors` or `fmt.Errorf`). Wrap errors as `errors.Wrap(err, "context")` — no "failed:" prefix in wrap messages.
- **Comments**: All comments must end with a period.
- **Formatting**: `gofumpt` + `goimports` with local prefix `github.com/go-faster/oteldb`.
- **Commits**: Conventional commits format: `type(scope): subject` (e.g., `fix(chstorage): fix column mapping`).
- Follow the [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md).

## Architecture

### Ingestion flow
```
Telemetry (OTLP/Prometheus RW)
  → cmd/oteldb (wiring)
  → internal/otelreceiver (translate & batch)
  → internal/otelstorage (normalize IDs/attributes)
  → internal/chstorage (inserters → ClickHouse SQL)
  → ClickHouse (logs / traces_spans / metrics_points tables)
```

### Query flow
```
API request (PromQL/LogQL/TraceQL)
  → internal/promapi | internal/logql | internal/traceql (parse & translate)
  → internal/chstorage queriers (SQL generation)
  → ClickHouse
```

### Key packages

| Package | Responsibility |
|---|---|
| `cmd/oteldb` | Main binary entry point; wires receivers, storage, and query servers |
| `cmd/otelproxy` | Proxy that forwards telemetry; useful for testing |
| `cmd/otelbench` | Benchmarking tool |
| `internal/chstorage` | ClickHouse storage layer: schema, inserters, queriers |
| `internal/otelreceiver` | OTLP and Prometheus RW receivers; translation to internal types |
| `internal/otelstorage` | ID/hash/timestamp helpers, attribute normalization |
| `internal/logql` | LogQL parser and translator |
| `internal/traceql` | TraceQL parser and translator |
| `internal/promapi` | Prometheus-compatible API + PromQL translation |
| `internal/logstorage` | Domain logic for logs on top of chstorage |
| `internal/tracestorage` | Domain logic for traces on top of chstorage |
| `internal/metricstorage` | Domain logic for metrics on top of chstorage |
| `internal/ddl` | DDL generator: `Table`/`Column`/`Index` types → ClickHouse SQL |

### ClickHouse schema

Three main tables (definitions in `internal/chstorage/_golden/*.sql`):
- **`logs`** — daily partitioned by `toYYYYMMDD(timestamp)`, ordered by `(severity_number, service_namespace, service_name, resource, timestamp)`.
- **`traces_spans`** — ordered by `(service_namespace, service_name, resource, start)`.
- **`metrics_points`** — daily partitioned, ordered by `(name, mapping, resource, attribute, timestamp)`.

All tables have a 3-day TTL (`259200` seconds).

### Schema change checklist
When adding/modifying columns:
1. Edit `internal/chstorage/_golden/*.sql` (source of truth)
2. Update `internal/chstorage/columns_*.go` (column mappings)
3. Update `internal/chstorage/inserter_*.go` (write path)
4. Update `internal/chstorage/querier_*.go` (read path)
5. Update `internal/chstorage/backup_*.go` (backup tool)
6. Update `internal/chstorage/restore_*.go` (backup tool)
7. Run `go test ./internal/chstorage/...` to verify golden files

### Golden file tests
`internal/chstorage/gold_test.go` compares generated SQL against `_golden/*.sql`. If you change schema code, run these tests and update golden files as needed.


### AI/Coding agent PR Guidelines

- Never push directly to `main`, `master` or `develop`
- Always create a new branch named `claude/<issue-number>-<short-description>`
- Open a draft PR referencing the issue number
