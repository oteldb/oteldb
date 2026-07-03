# embedded-bench — realistic oteldb PromQL benchmark

A self-contained, docker-compose PromQL benchmark of oteldb's **embedded** storage engine
(`--embedded`), built for iterating on storage/engine changes with numbers directly comparable to
the `oteldb` / `oteldb-s3` columns of
[`benchmark/results/REPORT.md`](../../../../benchmark/results/REPORT.md).

It benches **both storage backends side by side off one live ingest stream**: the local `file`
backend (`oteldb`) and the shared **go-faster/fs S3** object store (`oteldb-s3`, the same
`ghcr.io/go-faster/fs` substrate Mimir/Loki/Tempo use in the cross-engine benchmark). A single run
produces a side-by-side REPORT.md so you can see the object-store read-path cost against the local
`file` baseline.

It reproduces the benchmark's full realistic path — **live `node_exporter` → `vmagent`
(`BENCH_NODES` synthetic hosts) → Prometheus remote-write → oteldb under `GOMAXPROCS`/`GOMEMLIMIT`
caps, queried over the Prometheus HTTP API** — without standing up the whole benchctl matrix.

## Why this exists

`internal/storagebackend/bench_suite_test.go` measures **server-side engine cost** in-process (no
network, no caps). That is great for fast correctness checks and isolating the storage regression,
but its p90 is **not** comparable to `REPORT.md`: `REPORT.md` measures the full HTTP round-trip over
a CPU/memory-capped container with a ~140k-series dataset. This compose closes that gap — same
ingest, same caps, same suite, same HTTP measurement.

The critical realism knob (the same one that makes the in-process test fast): **`storage.backend`
must be `file`**, not the `--embedded` default (`memory`). `memory` keeps everything in head (no
parts, no decode) → sub-ms, unrepresentative. `oteldb.yml` here pins `file` on a volume so the head
flushes to compressed parts every 10s and queries decode+merge across them — the cost that dominates
`REPORT.md` p90.

## Run

From this directory (`dev/local/embedded-bench`):

```bash
go run .                          # nodes=100, prewarm 2m30s, 20 runs/query — both backends

go run . -gomaxprocs 2 -gomemlimit 1GiB   # tighten caps
go run . -prewarm 70m             # also fully populate the [30m]/[1h] instant selectors
go run . -cleanup=false           # leave the stack up (re-run bench / grab pprof)
go run . -s3=false                # bench only the file backend (skip the S3 instance)
go run . -nodes 10 -prewarm 15s   # quick smoke run (numbers NOT comparable — window under-populated)
```

`go run . -h` lists all flags. Every flag also has an env equivalent (`BENCH_NODES`,
`BENCH_RUNS`, `GOMAXPROCS`, `GOMEMLIMIT`, `PREWARM`, `BENCH_S3`, …); flags win.

**Prewarm must cover the lookback.** The range queries evaluate over `[now-lookback, now]`, and the
measured cost is dominated by how many flushed parts that window spans. A `-prewarm` shorter than
`-lookback` leaves the window mostly empty, so latencies read far too low and the S3 read path is
barely exercised (`s3 ≈ file`) — not comparable to `REPORT.md`. The default (`2m30s`) covers the
`120s` lookback with margin; the orchestrator warns if you set `-prewarm` below `-lookback`.

| flag          | default  | meaning                                                          |
|---------------|----------|------------------------------------------------------------------|
| `-nodes`      | `100`    | synthetic node_exporter hosts vmagent fans out (`~1400 series ×`)|
| `-prewarm`    | `2m30s`  | ingest window before querying — keep ≥ `-lookback` (see above)   |
| `-runs`       | `20`     | measured runs per query (after warmup)                           |
| `-warmup`     | `5`      | unmeasured warmup runs                                           |
| `-lookback`   | `120s`   | range-query window back from now                                 |
| `-concurrency`| `8`      | concurrent in-flight queries for the verify pass                 |
| `-gomaxprocs` | `4`      | oteldb CPU cap (matches `BENCH_CPUS`)                            |
| `-gomemlimit` | `1GiB`   | oteldb soft memory cap (matches `BENCH_MEMLIMIT`)               |
| `-health-wait`| `120s`   | max wait for oteldb `/liveness`; tails logs + status on timeout  |
| `-cleanup`    | `true`   | `false` leaves the stack up (re-run / pprof without re-ingest)   |
| `-s3`         | `true`   | also bench the S3 (go-faster/fs) backend; `-s3=false` skips it   |
| `-addr-s3`    | `:9093`  | oteldb-s3 PromQL API address (file backend is `-addr`, `:9090`)  |

## How it builds

`go run .` builds two binaries **on the host** from the oteldb repo root — `oteldb` and `otelbench` —
because the oteldb `go.mod` has `replace` directives pointing at the local `storage` and
`promql-engine` sources (absolute host paths), which only resolve on the host, never inside a
container. The single host-built `oteldb` binary backs **both** instances — the `file` and `s3`
services COPY the same binary into alpine, differing only in their mounted config (`oteldb.yml` vs
`oteldb-s3.yml`). The compose image is thin (`Dockerfile` just `COPY oteldb`), and the bench phase
invokes the host-built `otelbench` directly, so a run never rebuilds in-container or recompiles
otelbench per run.

## Profiling

With the stack up (`CLEANUP=0`), the `file` oteldb's pprof endpoint is on `:9010` (the `oteldb-s3`
instance's is on `:9012`):

```bash
go tool pprof -http=: http://127.0.0.1:9010/debug/pprof/profile?seconds=30
# drive the suite from another terminal while it captures:
go run github.com/oteldb/oteldb/cmd/otelbench promql bench -a http://127.0.0.1:9090 \
  -i queries.promql.yml --warmup 0 --count 50 --allow-empty
```

This is how the per-query findings in `benchmark/results/pprof/FINDINGS.md` were captured; the
hotspot locations it names (`engine.sortedWindow`, `recordCols`, `DecodeAttributes`) live in
`/src/oteldb/storage` and this oteldb checkout.

## What this is not

- Not a cross-engine comparison — only oteldb is brought up (its `file` and `s3` backends). For the
  head-to-head matrix (VictoriaMetrics / Mimir / …), use `benchctl` in `/src/oteldb/benchmark`.
- Not a static dataset — it ingests live, current-timestamp data. That's the point (no stale-timestamp
  problems), but it means two runs differ in exact sample counts; compare percentiles, not raw.
