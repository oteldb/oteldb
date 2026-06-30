# embedded-bench — realistic oteldb PromQL benchmark

A self-contained, docker-compose PromQL benchmark of oteldb's **embedded** storage engine
(`--embedded`, `file` backend), built for iterating on storage/engine changes with numbers
directly comparable to the `oteldb` column of
[`benchmark/results/REPORT.md`](../../../../benchmark/results/REPORT.md).

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
go run .                          # nodes=10, prewarm 10s, 20 runs/query

go run . -nodes 100               # match REPORT.md cardinality (~140k series)
go run . -gomaxprocs 2 -gomemlimit 1GiB   # tighten caps
go run . -prewarm 70m             # fully populate [30m]/[1h] range vectors
go run . -cleanup=false           # leave the stack up (re-run bench / grab pprof)
go run . -replace                 # rebuild oteldb, swap it in place, re-bench (no re-ingest)
```

`go run . -h` lists all flags. Every flag also has an env equivalent (`BENCH_NODES`,
`BENCH_RUNS`, `GOMAXPROCS`, `GOMEMLIMIT`, `REPLACE`, …); flags win.

| flag          | default | meaning                                                          |
|---------------|---------|------------------------------------------------------------------|
| `-nodes`      | `10`    | synthetic node_exporter hosts vmagent fans out (`~1400 series ×`)|
| `-prewarm`    | `10s`   | ingest window before querying (raise to `70m` for `[1h]` queries)|
| `-runs`       | `20`    | measured runs per query (after warmup)                            |
| `-warmup`     | `5`     | unmeasured warmup runs                                            |
| `-lookback`   | `120s`  | range-query window back from now                                  |
| `-concurrency`| `8`     | concurrent in-flight queries for the verify pass                  |
| `-gomaxprocs` | `4`     | oteldb CPU cap (matches `BENCH_CPUS`)                             |
| `-gomemlimit` | `2GiB`  | oteldb soft memory cap (matches `BENCH_MEMLIMIT`)                 |
| `-health-wait`| `120s`  | max wait for oteldb `/liveness`; tails logs + status on timeout   |
| `-cleanup`    | `true`  | `false` leaves the stack up (re-run / pprof without re-ingest)    |
| `-replace`    | `false` | rebuild oteldb, swap it in place (stack already up), re-bench     |

## How it builds

`go run .` builds two binaries **on the host** from the oteldb repo root — `oteldb` and `otelbench` —
because the oteldb `go.mod` has `replace` directives pointing at the local `storage` and
`promql-engine` sources (absolute host paths), which only resolve on the host, never inside a
container. The compose image is thin (`Dockerfile` just `COPY oteldb` into alpine), and the bench
phase invokes the host-built `otelbench` directly, so the loop never rebuilds in-container or
recompiles otelbench per run.

## The replace loop

Once the stack is up (`-cleanup=false`), iterate on oteldb source with:

```bash
go run . -replace
```

It rebuilds the `oteldb` binary, rebuilds the one-copy-layer image, and swaps just the oteldb
container (`--force-recreate --no-deps oteldb`) while node-exporter / vmagent / the data volume keep
running. vmagent keeps remote-writing, so the new oteldb's head refills within a couple scrapes.
Fresh numbers in the time `go build` + one image layer takes.

## Profiling

With the stack up (`CLEANUP=0`), oteldb's pprof endpoint is on `:9010`:

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

- Not a cross-engine comparison — only `oteldb` is brought up. For the head-to-head matrix
  (VictoriaMetrics / Mimir / …), use `benchctl` in `/src/oteldb/benchmark`.
- Not a static dataset — it ingests live, current-timestamp data. That's the point (no stale-timestamp
  problems), but it means two runs differ in exact sample counts; compare percentiles, not raw.
