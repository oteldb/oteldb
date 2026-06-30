# embedded-bench — latency investigation & optimization plan

Captured against a live `embedded-bench` stack (`--embedded`, `file` backend,
GOMAXPROCS=4, GOMEMLIMIT=2GiB, nodes=100 ⇒ ~140k series, prewarm=10s,
lookback=120s, 20 runs/query). Raw profiles saved in `pprof/`:

- `pprof/cpu.pb.gz` — 30s CPU under query load.
- `pprof/allocs.pb.gz` — 20s `alloc_space` delta under load.
- `pprof/heap.pb.gz` — in-use heap snapshot.

Reproduce with the stack up (`-cleanup=false`):

```bash
go run . -cleanup=false                         # bring up + bench
./otelbench promql bench --addr http://127.0.0.1:9090 \
  -i queries.promql.yml --warmup 0 --count 40 \
  --start $(( $(date +%s) - 120 )) --end $(date +%s) -o /dev/null   # drive load
curl -o cpu.pb.gz    'http://127.0.0.1:9010/debug/pprof/profile?seconds=30'
curl -o allocs.pb.gz 'http://127.0.0.1:9010/debug/pprof/allocs?seconds=20'
go tool pprof -top -cum        pprof/cpu.pb.gz
go tool pprof -top -cum -sample_index=alloc_space pprof/allocs.pb.gz
```

pprof is served by `go-faster/sdk` off `PPROF_ADDR=0.0.0.0:9010`; the binary
for symbolication is `./oteldb` (host-built, identical to the container copy).

## Current latencies (prewarm=10s)

| query | p50 | p90 | p99 | max |
|---|---:|---:|---:|---:|
| `cpu usage ratio` | 85.8 | 113.0 | 162.0 | 162.0 |
| `cpu wait time` | 15.8 | 18.5 | 23.6 | 23.6 |
| `count cpu cores` | 81.5 | 95.5 | 112.4 | 112.4 |
| `disk read bytes rate` | 2.2 | 2.7 | 3.4 | 3.4 |
| `load1 0.99 quantile over time` | 0.7 | 1.0 | 1.3 | 1.3 |
| `network receive rate` | 1.0 | 1.3 | 1.8 | 1.8 |
| `top 3 free memory` | 0.6 | 0.9 | 1.1 | 1.1 |
| `worst case full series count` | 317.8 | 361.1 | 480.5 | 480.5 |

Five queries are already single-digit ms. The slow outliers are:

1. `count({__name__=~"node_.+"})` — 317 ms p50. A regex-name label scan over
   the full `node_` namespace; touches every live part and every series.
2. `count(count(node_cpu_seconds_total) by (cpu))` — 81 ms p50. Nested
   aggregation with a `by (cpu)` re-group; one fetch per outer + per inner
   series group.
3. `sum(irate(...[1m])) / sum(irate(...[1m]))` (cpu usage ratio) — 85 ms p50.
   Two `irate` subexpressions over `node_cpu_seconds_total` (the largest
   metric), evaluated and joined.
4. `irate(node_schedstat_waiting_seconds_total[1m])` — 15 ms p50.

Caveat: `top 3 free memory` (`avg_over_time(...[30m])`) and `load1 … over time`
(`quantile_over_time(...[1h])`) only look fast because prewarm=10s
under-populates their `[30m]`/`[1h]` windows. With `-prewarm 70m` they would
decode far more rows and dominate, exactly as the older REPORT.md showed.
The root cause below is the same either way; prewarm just changes how many
parts a range vector fans out to.

## Root cause: decode-cache thrash → re-decode every fetch

The headline number from the allocs profile (20s, ~40 queries):

```
flat  flat%   sum%       cum    cum%
0     0%     0%   38.13GB  80.63%  github.com/oteldb/storage/engine.(*Engine).decodeOf
0     0%     0%   38.13GB  80.63%  github.com/oteldb/storage/engine.(*part).decodeInto
0     0%     0%   18.05GB  38.16%  github.com/oteldb/storage/block.decodeColumn[go.shape.float64]
0     0%     0%   22.98GB  48.58%  sync.(*Once).doSlow      # per-fetch decode-once memo
```

**80% of all allocations (38 GB) are in `decodeOf`/`decodeInto`.** CPU tells
the same story: decode + GC together own most of the flat time, with the XOR
delta-of-delta decoders as the leaf hotspots:

```
chunk.DecodeFloats            20.98% cum  (2.88s flat)
chunk.DecodeTimestamps        17.21% cum  (2.32s flat)
chunk.xorReadControl           7.03% cum  (3.18s flat)
chunk.readDoDPrefix            6.93% cum  (2.95s flat)
chunk.xorRead                 13.07% cum
runtime.scanObject            14.69% cum  (4.84s flat)   # GC marking decode buffers
runtime.memclrNoHeapPointers   5.97% flat
```

### Why the cache doesn't help

The engine has the right machinery:
`Engine.decodeOf` (`storage/engine/engine.go:982`) memoizes decoded parts in
a cross-fetch LRU `decodeCache`, and a per-fetch `partDecodeCache`
(`engine/part.go:314`) makes each part decode once per fetch.

The problem is sizing. The decoded-part cache defaults to `1/32` of the Go
memory limit, floor 64 MiB (`cmd/oteldb/storage_backend.go:256`). With
`GOMEMLIMIT=2GiB` that resolves to exactly **64 MiB** (log line confirms:
`decode_cache_bytes:67108864`).

A decoded part holds the *whole* `ts` + `value` (+ optional `sf`) columns
(`part.go:236` `decodeInto`), not the per-series slices the merge actually
consumes. For a 10s flush of ~140k series at a 2s scrape interval that is
~700k rows × (8B ts + 8B value) ≈ **11–16 MiB per decoded part**. The cache
therefore holds ~4 parts; the query window (`[1m]`/`[5m]` ranges, lookback
120s, flush every 10s) touches ~12–30 live parts, and the full-namespace
queries touch *every* live part (104 part dirs on disk, ~20 recent). So every
fetch evicts and re-decodes, the cache miss rate is effectively 100%, and the
XOR/bitstream decode cost is paid on each of the 160 benchmark runs. The
decoded buffers are then immediately discarded → the GC churn (`scanObject`
14.7%, `memclr` 6%) seen in the CPU profile.

The same flow explains the old REPORT.md's slow `[30m]`/`[1h]` queries: more
range ⇒ more parts touched ⇒ more re-decode; the dataset here just doesn't
exercise those windows yet.

### Secondary: `sync.Once` at 48% of allocs

`sync.(*Once).doSlow` is 48.6% of allocs. That is the per-fetch
`partDecodeCache.get` path: on its first access to a given part a fetch calls
`Engine.decodeOf`, which (on a cross-fetch cache miss) calls `part.decode`.
Because the cross-fetch cache thrashes, the "first access" happens for nearly
every part on every fetch, so the one-time `Once` becomes per-fetch-per-part
cost. Fix the cache and this line collapses with `decodeInto`.

## Optimization plan (ordered by leverage)

### 1. Make the decode cache actually cover the working set — biggest win

The single highest-leverage change: stop re-decoding. Two complementary
moves, do both:

- **a. Size the default decode cache to the working set, not 1/32 of RAM.**
  `defaultDecodeCacheBytes` (`cmd/oteldb/storage_backend.go:255`) at floor
  64 MiB is ~4 parts — far below any realistic query window. Either raise the
  floor (e.g. 256–512 MiB) or size it off the live part set rather than a
  RAM fraction. A 256 MiB cache would hold ~16–22 parts and turn the steady-
  state `cpu`/`count` queries from 80%+ re-decode to almost all cache hits.
  Low risk, config-only; start here and re-bench.

- **b. Decode per-series rows, not whole columns.** `part.decodeInto`
  (`engine/part.go:236`) decodes the entire `ts`/`value`/`sf` columns for
  the whole part even when the merge will only read one series' `rowRange`
  slice out of it. For selectors that touch a small fraction of a part's
  series (the `cpu`/`network`/`disk` matchers, which hit ~1k of ~140k
  series), decoding whole columns is pure waste. Add a range-aware decode
  entry point `decodeRange(rng)` that reads just `[rng.start:rng.end)` per
  column (the chunk codec already indexes on row offsets), and have
  `mergeSeries` call it when the touched fraction is small (or always, with
  the whole-column decode kept as the cache payload for dense queries). This
  cuts both decode CPU and the per-decode allocation that drives the cache
  thrash in the first place.

Either of these alone materially helps; (a) is trivial and should be applied
first to confirm the thrash hypothesis, (b) is the durable fix and also
shrinks resident heap.

### 2. Cache `count` / `count by (...)` aggregates via the stats sidecar

`worst case full series count` (`count({__name__=~"node_.+"})`, 317 ms) and
`count cpu cores` (`count(count(...) by (cpu))`, 81 ms) are pure card/count
aggregations. The engine already has an aggregate-pushdown sidecar
(`part.statsOnce`; `engine.WithAggregateStats` is enabled by default —
`storage_backend.go:227`). Verify the `count({...})` and `by (...)`-group
paths actually hit it; the regex `__name__=~"node_.+"` matcher in particular
often bypasses name-keyed pushdowns and falls back to a full scan. Harbor the
common `__name__` regex into a name-set pre-filter so the sidecar resolves
without decoding. Target: bring these two onto the single-digit-ms track the
engine already shows for `network`/`disk`.

### 3. Pool decode buffers on the no-cache / miss path

When the decode cache does miss (cold start, first run), `part.decode`
(`engine/part.go:236`) allocates fresh `[]int64`/`[]float64` every time and
the cross-fetch path never pools them (`decodeOf` line 1008 vs the pooled
no-cache branch at 987). Wire the cached path to also draw from
`Engine.decPool` and recycle on `releaseParts`, matching the no-cache branch.
Kills the GC churn (`scanObject` 14.7%, `memclr` 6%) seen in the CPU profile.
Small change, broad benefit across all signals.

### 4. (Later) codec-level micro-opts in `chunk.DecodeFloats`/`DecodeTimestamps`

Once decode stops repeating, the remaining cost is the XOR decode itself.
`chunk.readDoDPrefix` (6.9%), `xorReadControl` (7%), `bitstream.ReadBits`
(4%) are branchy; there is headroom to vectorize the DoD prefix loop and/or
batch `ReadBits`. Low priority — the above items remove ~80% of current
cost by avoiding the call entirely.

## What is NOT the bottleneck

`engine.sortedWindow` (the previous report's #1) is gone from the top — it
does not appear in either profile's top 25. The `appendSample`/`recordCols`
resident-heap costs named in past findings are ingest-side and do not show
on this read-bound bench. The work is now squarely on the **decode path**;
the levers above are ordered by how much of the 80% alloc / ~50% CPU they
remove.

## Validation

After each change, re-run the loop and compare against the table at the top:

```bash
go run . -replace               # rebuild oteldb, swap in place, re-bench
# expected: cpu/count queries drop toward the disk/network band (single-digit ms)
```

For a fair `[30m]`/`[1h]` comparison also run `-prewarm 70m`. Check the cache
hit rate (expose `DecodeCacheStats` — `storage/engine/decodecache.go:120` —
on `/metrics`; currently not scraped) before/after change #1 to confirm the
thrash-to-hit flip.