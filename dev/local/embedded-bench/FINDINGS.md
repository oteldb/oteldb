# embedded-bench — latency investigation, fix, and A/B validation

The decode path was the bottleneck; the decode **cache** was the lever. This
session confirmed that with a controlled A/B and applied the floor-raise fix.

- **Root cause:** the decoded-part cache thrashes — at the old 64 MiB floor the
  working set never fits, so every fetch re-decodes (80% of allocs in
  `decodeOf`). Profile evidence below.
- **Fix applied:** raise the default decode-cache floor 64 MiB → 512 MiB
  (`cmd/oteldb/storage_backend.go:260`, `defaultDecodeCacheBytes`). This is what
  an *uncapped* process actually gets (no `GOMEMLIMIT` ⇒ `SetMemoryLimit(-1)`
  returns `MaxInt64` ⇒ the floor branch).
- **Validated:** a same-binary A/B (only `decode_cache_bytes` differs) shows
  **14–100×** speedups on the cache-bound queries. Table below.
- A separate **correctness** fix shipped in the same build — instant
  `count(selector)` returned the empty vector on the v0.43.0 release. See
  `COUNT_BUG.md`; both fixes ride in `oteldb:head-89350324`
  (oteldb HEAD + local `promql-engine`/`storage`).

## A/B: decode cache 512 MiB vs 64 MiB — the proof

Driven through `/src/oteldb/benchmark` (`benchctl bench metrics 20 --only oteldb`)
against `oteldb:head-89350324`, **uncapped** (32 cores, no `GOMEMLIMIT`), file
backend, 100 synthetic hosts ⇒ 142800 series, 30s prewarm. Same binary, same
harness, same queries — the *only* difference is `storage.decode_cache_bytes`
(verified in the boot log: `67108864` vs `536870912`). Correctness in both
runs: **8 OK, 0 failed**.

| query | **512 MiB** p50 | **64 MiB** p50 | slowdown at 64 MiB |
|---|---:|---:|---:|
| `top 3 free memory` | 0.8 | 84.9 | **~100×** |
| `load1 0.99 quantile` | 1.1 | 67.8 | **~60×** |
| `network receive rate` | 2.3 | 68.6 | **~30×** |
| `disk read bytes rate` | 4.9 | 71.0 | **~14×** |
| `cpu wait time` | 30 | 98.5 | 3.3× |
| `count cpu cores` | 145 | 185 | 1.3× |
| `cpu usage ratio` | 176 | 232 | 1.3× |
| `worst case full series count` | 542 | 600 | 1.1× |
| RSS | 1398 MB | 969 MB | smaller cache ⇒ less RSS |

**Read this as two regimes:**

1. **Cache-bound queries** (`top`, `load`, `network`, `disk`). With 512 MiB their
   working set stays resident and they run sub-5 ms. With 64 MiB they all
   collapse onto a **~70–85 ms floor** — flat, regardless of what the query
   computes. That flat floor is the thrash signature: every fetch re-decodes the
   same parts, so the cost is decode, not query logic. The bigger cache is a
   14–100× win here, for free.

2. **Scan-bound queries** (`full_scan_count`, `cpu_usage`, `count_cpu_cores`).
   Only ~10–30% faster, because they touch the *entire* `node_` namespace / the
   largest metric and exceed even 512 MiB. A bigger cache can't help a working
   set that never fits — these need the **per-series decode** fix (§1b), not more
   cache.

**Confounder, stated honestly:** the two A/B runs scraped independent 30s
windows, so on-disk data differed (512 MiB run: 82 MiB; 64 MiB run: 126 MiB —
i.e. the *slower* run also had ~1.5× more data). That cannot explain a 100×
latency jump or the flat ~70 ms floor across unrelated queries, so the cache is
unambiguously the driver — but a rigorous A/B should replay a *frozen* dataset
into both rather than rely on live scrape. Treat the multipliers as
order-of-magnitude, not three-significant-figure.

## Root cause: decode-cache thrash → re-decode every fetch

Captured against a live embedded-bench stack (`--embedded`, file backend,
GOMAXPROCS=4, GOMEMLIMIT=2GiB, nodes=100 ⇒ ~140k series, prewarm=10s,
lookback=120s). Raw profiles in `pprof/` (`cpu.pb.gz`, `allocs.pb.gz`,
`heap.pb.gz`). Reproduce with the stack up (`-cleanup=false`):

```bash
go run . -cleanup=false
./otelbench promql bench --addr http://127.0.0.1:9090 \
  -i queries.promql.yml --warmup 0 --count 40 \
  --start $(( $(date +%s) - 120 )) --end $(date +%s) -o /dev/null
curl -o cpu.pb.gz    'http://127.0.0.1:9010/debug/pprof/profile?seconds=30'
curl -o allocs.pb.gz 'http://127.0.0.1:9010/debug/pprof/allocs?seconds=20'
go tool pprof -top -cum        pprof/cpu.pb.gz
go tool pprof -top -cum -sample_index=alloc_space pprof/allocs.pb.gz
```

The headline from the allocs profile (20s, ~40 queries):

```
flat  flat%   sum%       cum    cum%
0     0%     0%   38.13GB  80.63%  github.com/oteldb/storage/engine.(*Engine).decodeOf
0     0%     0%   38.13GB  80.63%  github.com/oteldb/storage/engine.(*part).decodeInto
0     0%     0%   18.05GB  38.16%  github.com/oteldb/storage/block.decodeColumn[go.shape.float64]
0     0%     0%   22.98GB  48.58%  sync.(*Once).doSlow      # per-fetch decode-once memo
```

**80% of all allocations (38 GB) are in `decodeOf`/`decodeInto`.** CPU agrees:
the XOR delta-of-delta decoders are the leaf hotspots, with GC marking the
decode buffers right behind them:

```
chunk.DecodeFloats            20.98% cum  (2.88s flat)
chunk.DecodeTimestamps        17.21% cum  (2.32s flat)
chunk.xorReadControl           7.03% cum  (3.18s flat)
chunk.readDoDPrefix            6.93% cum  (2.95s flat)
chunk.xorRead                 13.07% cum
runtime.scanObject            14.69% cum  (4.84s flat)   # GC marking decode buffers
runtime.memclrNoHeapPointers   5.97% flat
```

### Why the cache thrashed (the sizing math)

The engine has the right machinery: `Engine.decodeOf`
(`storage/engine/engine.go:982`) memoizes decoded parts in a cross-fetch LRU
`decodeCache`, and a per-fetch `partDecodeCache` (`engine/part.go:314`) makes
each part decode once per fetch.

A decoded part holds the *whole* `ts`+`value`(+`sf`) columns
(`engine/part.go:236` `decodeInto`), not the per-series slices the merge
consumes. For a 10s flush of ~140k series at a 2s scrape that is ~700k rows ×
16B ≈ **11–16 MiB per decoded part**. At the old 64 MiB floor the cache held
~4 parts; a `[1m]`/`[5m]` query window touches ~12–30 live parts, and the
full-namespace `count` touches *every* live part. So the miss rate was
effectively 100%, the XOR/bitstream decode was paid on every run, and the
discarded buffers drove the GC churn (`scanObject` 14.7%, `memclr` 6%). The
512 MiB floor holds ~32–46 parts, covering the steady-state windows — which is
exactly the 14–100× flip the A/B shows.

`sync.(*Once).doSlow` at 48.6% of allocs is the same story: the per-fetch
`partDecodeCache.get` "first access" path. Because the cross-fetch cache
thrashed, "first access" happened for nearly every part on every fetch, so the
one-time `Once` became per-fetch-per-part cost. It collapses with `decodeInto`
once the cache holds.

## Optimization plan (ordered by leverage)

### 1a. Raise the decode-cache floor 64 → 512 MiB — DONE, validated

`defaultDecodeCacheBytes` (`cmd/oteldb/storage_backend.go:260`) floor raised to
`512<<20`. Config-only, low risk. The A/B above is the confirmation: cache-bound
queries dropped from a ~70–85 ms thrash floor to sub-5 ms. Cost is ~430 MB more
RSS (969 → 1398 MB), which an uncapped box has. For capped deployments the
fraction path (`1/32` of `GOMEMLIMIT`) still applies above the floor.

### 1b. Decode per-series rows, not whole columns — the durable fix, NOT done

This is the remaining win, and the *only* lever for the scan-bound queries that
512 MiB can't cache (`full_scan_count` 542 ms, `cpu_usage` 176 ms,
`count_cpu_cores` 145 ms — all still slow with the big cache). `part.decodeInto`
(`engine/part.go:236`) decodes the entire `ts`/`value`/`sf` columns even when
the merge reads one series' `rowRange` out of it. For selectors touching a small
fraction of a part's series (the `cpu`/`network`/`disk` matchers hit ~1k of
~140k), whole-column decode is pure waste. Add a range-aware `decodeRange(rng)`
that reads just `[rng.start:rng.end)` per column (the chunk codec already indexes
on row offsets) and have `mergeSeries` call it when the touched fraction is
small. Cuts decode CPU *and* the per-decode allocation that drove the thrash —
and shrinks resident heap, partially offsetting 1a's RSS cost.

### 2. Cache `count` / `count by (...)` aggregates via the stats sidecar

`worst case full series count` (542 ms) and `count cpu cores` (145 ms) are pure
card/count aggregations. The engine has an aggregate-pushdown sidecar
(`part.statsOnce`; `WithAggregateStats` on by default,
`storage_backend.go:227`). Verify the `count({...})` and `by (...)`-group paths
actually hit it; the regex `__name__=~"node_.+"` matcher in particular often
bypasses name-keyed pushdowns and falls back to a full scan. (Note: the count
*correctness* bug is now fixed — `COUNT_BUG.md` — so this is purely about
latency, not the empty-result regression.)

### 3. Pool decode buffers on the miss path

On a cache miss `part.decode` allocates fresh `[]int64`/`[]float64` and the
cross-fetch path never pools them (`decodeOf` ~line 1008 vs the pooled no-cache
branch at ~987). Wire the cached path to draw from `Engine.decPool` and recycle
on `releaseParts`. Kills the residual GC churn. With 1a holding the cache this
matters most on cold start / scan-bound queries.

### 4. (Later) codec micro-opts in `chunk.DecodeFloats`/`DecodeTimestamps`

Once decode stops repeating, the floor is the XOR decode itself —
`readDoDPrefix` (6.9%), `xorReadControl` (7%), `bitstream.ReadBits` (4%) are
branchy and vectorizable. Low priority; 1a+1b remove most of the cost by
avoiding the call.

## What is NOT the bottleneck

`engine.sortedWindow` (a previous report's #1) is gone from the top 25 of both
profiles. The `appendSample`/`recordCols` resident-heap costs are ingest-side
and do not show on this read-bound bench. The work is squarely on the **decode
path**.

## Validation

After each change, re-run and compare against the A/B table:

```bash
# embedded-bench inner loop (host-built oteldb, file backend):
go run . -replace                # rebuild oteldb, swap in place, re-bench
# cross-engine bench (uncapped), oteldb only — what produced the A/B above:
cd /src/oteldb/benchmark && ./benchctl bench metrics 20 --only oteldb
```

For a fair `[30m]`/`[1h]` comparison raise prewarm to `70m` — the default 30s/10s
under-populates those windows (see `COMPARISON.md`). To re-pin the cache for an
A/B, set `storage.decode_cache_bytes` (e.g. `"64MiB"`) in the oteldb config and
recreate the container; confirm via the boot log's `decode_cache_bytes`. Expose
`DecodeCacheStats` (`storage/engine/decodecache.go:120`) on `/metrics` to watch
the hit-rate flip directly — currently not scraped.
