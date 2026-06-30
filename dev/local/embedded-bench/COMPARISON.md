# embedded-bench vs /src/oteldb/benchmark — setup & latency comparison

Both benches exercise the **same 8 PromQL queries** (verified: identical query
strings), the **same ingest path** (node_exporter → vmagent fan-out as 100
synthetic hosts → Prometheus remote-write), and the **same oteldb `--embedded`
+ `file` backend**. So why do their numbers differ — and why is the benchmark
oteldb *slower* than embedded-bench despite being uncapped?

## TL;DR

They are not measuring the same thing. Six factors differ; only one (the
decode-cache bug) is a real oteldb problem, the rest are harness choices:

| factor | embedded-bench | benchmark (live) | effect |
|---|---|---|---|
| **CPU/RAM cap** | GOMAXPROCS=4, GOMEMLIMIT=2GiB | **uncapped** (32 cores, no limit) | benchmark has ~8× cores; *should* be faster |
| **Prewarm / data volume** | 10s (default) | ~10 min live | **dominant** — benchmark decodes 10–30× more rows per range query |
| **Decode cache** | 64 MiB | **64 MiB** (same!) | uncapping RAM does **not** enlarge the cache — see bug below |
| **Binary** | built from local HEAD (`v0.42.0-38-g2975b557`) | prebuilt release image `ghcr.io/oteldb/oteldb:v0.43.0` | different code; release may carry fixes/regressions |
| **Query harness** | `otelbench promql bench` (in-process HTTP client, pipelined) | `benchctl query` = one `curl` GET per run (forks per query) | benchmark numbers carry ~tens of ms of fork overhead; not percentile-comparable |
| **oteldb config** | `max_samples 10M`, `timeout 5m`, no `log_query_parallelism` | `max_samples 1M`, `timeout 1m`, `log_query_parallelism 8` | minor for metrics lane |

## Live numbers from the running benchmark oteldb

Measured with a clone of `benchctl query`'s own method (sequential GET,
120s lookback window, step 15, 3 warmup + 15 measured runs) against the live
`http://127.0.0.1:9090` (uncapped, ~10 min of data, 197 parts / 222 MiB on
disk). Alongside embedded-bench's 10s-prewarm numbers for contrast:

| query | benchmark (uncapped, ~10min) | embedded-bench (4c/2GiB, 10s) |
|---|---:|---:|
| `worst case full series count` | 320 | 318 |
| `count cpu cores` | 690 | 83 |
| `cpu usage ratio` | 600 | 100 |
| `cpu wait time` | 580 | 18 |
| `network receive rate` | 840 | 1.4 |
| `disk read bytes rate` | 810 | 3.1 |
| `top 3 free memory` ([30m]) | 800–1060 | 0.8 |
| `load1 quantile` ([1h]) | 1000 | 1.0 |

(The benchmark column includes ~10–30 ms of `curl`+`/usr/bin/time` fork
overhead per request, so subtract that for the true server cost — but the
orders of magnitude hold.)

**Read this table as:** the only query that matches is `full_scan_count`
(instant, scans whatever is live, ~320 ms either way — decode-cache thrash is
CPU-bound, not core-count-bound). Everything else is 1–3 orders of magnitude
slower on the benchmark because its range vectors (`[1m]`, `[5m]`, `[30m]`,
`[1h]`) are actually *populated* — embedded-bench's 10s prewarm leaves them
nearly empty, so `rate(...[5m])` decodes a handful of samples instead of
~150k series × 30 scrapes.

In other words: **embedded-bench with `-prewarm 10s` is not a valid
comparison to `REPORT.md`-style numbers.** Its own README says so (`-prewarm
70m` for `[1h]` queries); the default just happens to make the dashboard look
fast. Re-run embedded-bench with `-prewarm 70m` and the slow queries land in
the same hundreds-of-ms band the benchmark shows.

## The one real bug both benches share: decode-cache floor

The benchmark's `shared.yml` deliberately leaves oteldb uncapped
(`GOMAXPROCS=${BENCH_CPUS:-}`, `GOMEMLIMIT=${BENCH_MEMLIMIT:-}` — both empty),
with a comment explaining capping only oteldb is unfair. But the decode-cache
defaulting logic defeats that intent. From `cmd/oteldb/storage_backend.go:261`:

```go
func defaultCacheBytes(fraction float64, minBytes int64) int64 {
    limit := debug.SetMemoryLimit(-1)
    if limit <= 0 || limit == math.MaxInt64 {
        return minBytes          // <-- no GOMEMLIMIT set ⇒ floor (64 MiB)
    }
    ...
}
```

So with `GOMEMLIMIT` unset, `debug.SetMemoryLimit(-1)` returns `MaxInt64`,
and the decode cache collapses to its **64 MiB floor** regardless of how much
RAM the box has. Confirmed in both containers' logs:

```
benchmark:     "decode_cache_bytes":67108864   (uncapped, 32-core box, 222 MiB parts)
embedded-bench:"decode_cache_bytes":67108864   (GOMEMLIMIT=2GiB ⇒ 1/32 = exactly 64 MiB)
```

That is why uncapping the benchmark oteldb buys almost nothing: the working
set (~12–30 parts × ~11–16 MiB decoded each ≈ 150–480 MiB) far exceeds the
64 MiB cache, so every fetch re-decodes, and more cores only parallelize the
redundant work. See `FINDINGS.md` § "Root cause" for the profile evidence
(80% of allocs in `decodeOf`).

## How to make them comparable

To get a like-for-like number, align the harnesses:

1. **Same prewarm.** Run `embedded-bench -prewarm 70m` (or shorter if you
   only care about `[5m]` queries: `-prewarm 8m`). The default 10s produces
   numbers that are not comparable to `REPORT.md`.
2. **Same harness.** Drive both with the same client. Easiest: point
   `otelbench promql bench` at the benchmark oteldb too
   (`--addr http://127.0.0.1:9090`), so the HTTP-client/fork difference
   drops out. `benchctl query` forks `curl` per request; `otelbench` keeps a
   pooled in-process client — they disagree by tens of ms at the low end.
3. **Same caps.** Either cap the benchmark oteldb
   (`BENCH_CPUS=4 BENCH_MEMLIMIT=2GiB`) or uncap embedded-bench
   (`-gomaxprocs 0 -gomemlimit 0` won't do it — set `GOMEMLIMIT` empty).
   Given the cache bug above, prefer capping the benchmark to match
   embedded-bench until the cache is fixed; otherwise you're comparing
   "uncapped but cache-starved" against "capped and cache-starved".
4. **Same binary.** The benchmark runs release `v0.43.0`; embedded-bench
   builds from your checkout HEAD. To attribute a delta to code rather than
   setup, rebuild the benchmark's oteldb image from the same checkout
   (`OTELDB_IMAGE=... benchctl up`, or `docker build` + override) and
   re-run both.

## What each bench is actually good for

- **`/src/oteldb/benchmark`** — the cross-engine matrix (oteldb vs Prometheus
  vs Mimir vs VM vs …). Apples-to-apples ingest, identical caps across
  engines, full dataset. Use it to know where oteldb ranks. Its oteldb numbers
  are the "real" ones (full prewarm, REPORT.md-comparable).
- **`dev/local/embedded-bench`** — the fast oteldb-only inner loop. Builds
  from local source, `-replace` swaps a rebuilt binary in seconds without
  re-ingesting. Use it to iterate on storage/engine changes with pprof at
  `:9010`. Just don't trust the default `-prewarm 10s` latencies for the
  range-vector queries — raise it to `70m` before drawing conclusions.

Both share the same root performance bug (64 MiB decode cache floor), so fix
#1 in `FINDINGS.md` (size the decode cache to the working set, not the
RAM-fraction/floor) improves both benches' numbers by the same mechanism.