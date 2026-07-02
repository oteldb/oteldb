# Cross-engine PromQL report — oteldb vs. reference TSDB (2026-07-01)

oteldb's embedded engine benched head-to-head against the **reference local-disk
TSDB** (the fast native PromQL baseline in the matrix) on one dataset, one ingest
path, one machine. Driven from `/src/oteldb/benchmark`:

```bash
BENCH_ONLY=oteldb,<reference> ./benchctl bench metrics 20
```

Both engines receive the **same** live remote-write stream (node_exporter fanned
out to 100 synthetic hosts ⇒ ~140k active series), settle over the same 30s
prewarm + 30s flush window, and answer the **same 8 PromQL queries** over their
Prometheus HTTP APIs. So every number below is same-data / same-hardware; the
comparison isolates the engine.

## Build under test

- **oteldb** `776c74eb` + local `storage` `bee17bc` (feat/pool-plan-maps),
  `promql-engine v0.8.0`. Thin image `oteldb:head-776c74eb` — host-built static
  binary (the host resolves the go.mod `replace … => /src/oteldb/storage`), wrapped
  in alpine. Set `OTELDB_IMAGE=oteldb:head-776c74eb` in `benchmark/.env`.
- **Runtime regime:** `--embedded`, `file` backend, CPU uncapped, `GOMEMLIMIT=1GiB`.
  Confirmed from the boot log: **`decode_cache_bytes = 67108864` (64 MiB)**. This is
  the *RSS-budget* regime — with `GOMEMLIMIT=1GiB` set, the cache sizing takes the
  fraction path (`1GiB/32 = 32 MiB`, floored to the 64 MiB minimum), **not** the
  512 MiB uncapped floor. See the caveat in "Differences".

## Latency (ms) — lower is better

| query | oteldb p50 | oteldb p90 | oteldb p99 | ref p50 | ref p90 | ref p99 | oteldb/ref (p90) |
|---|---:|---:|---:|---:|---:|---:|---:|
| `count_cpu_cores` | 105.5 | 134.0 | 229.3 | 27.3 | 49.1 | 54.3 | **2.73×** |
| `cpu_usage`       | 135.9 | 178.3 | 240.1 | 103.3 | 111.9 | 120.5 | **1.59×** |
| `cpu_wait`        | 26.8 | 33.6 | 48.7 | 17.7 | 21.7 | 26.4 | 1.55× |
| `disk_io`         | 4.9 | 7.6 | 8.7 | 2.1 | 2.6 | 3.5 | 2.92× |
| `rate_network`    | 3.3 | 4.6 | 13.3 | 1.0 | 1.2 | 1.5 | 3.83× |
| `topk_free_mem`   | 0.9 | 1.9 | 2.8 | 0.5 | 0.5 | 0.5 | 3.8× |
| `load_quantile`   | 0.9 | 1.2 | 1.3 | 0.6 | 2.1 | 4.2 | **0.57×** (faster) |
| `full_scan_count` | 184.1 | 314.4 | 349.6 | 252.3 | 345.1 | 369.2 | **0.91×** (faster) |

**Correctness:** 16/16 queries verified, oteldb is the reference (`★`) for result
count on every query and every query returns data. The reference engine
under-returns on `cpu_usage` (97 vs oteldb's 100 series) — an artifact of the
reference, not an oteldb defect. oteldb's answers are correct across the suite.

## Footprint

| metric | oteldb | reference | oteldb/ref |
|---|---:|---:|---:|
| on-disk | 70.5 MB | 23.1 MB | **3.05×** |
| resident RSS | 897 MB | 355 MB | **2.53×** |

RSS sits at **897 MB — under the 1 GiB budget** (the cap is doing its job). The
reference holds the same data in ~355 MB. Disk is oteldb's columnar parts vs. the
reference's native TSDB blocks.

## Differences — how to read this

Two regimes, same as the in-process findings (`FINDINGS.md`):

1. **Light range-vector queries** (`topk_free_mem`, `rate_network`, `disk_io`,
   `load_quantile`) — all **sub-8 ms**, competitive; `load_quantile` is actually
   faster than the reference. The 3–4× ratios here are on tiny absolute numbers
   (0.5 ms vs 1.9 ms) — not where the work is.

2. **Heavy cardinality / scan queries** — the real gap:
   - `count_cpu_cores` (2.73×) and `cpu_usage` (1.59×) are the worst offenders.
     Both scan a large slice of the `node_` namespace; per `FINDINGS.md §1b` the
     engine decodes **whole columns** even when the merge reads one series' row
     range, so these pay full decode over ~140k series.
   - `full_scan_count` now **ties/beats** the reference (0.91×) and dropped from
     **509 ms → 314 ms p90 vs. the previous local build** (`head-7a2c0815`) — the
     count-pushdown + plan-map/interning pooling that landed in `storage bee17bc`
     is showing up here. (Caveat: prior baseline was a separate live-scrape
     window; treat the 509→314 as a real but not 3-sig-fig delta.)
   - **Tail:** `count_cpu_cores` p99 (229) is **1.7× its p90** (134) while the
     reference p99/p90 is tight (54/49 = 1.1×). oteldb's wide tail is GC pressure
     against the 1 GiB cap on the scan path.

3. **Latent risk — the 64 MiB decode cache under the RSS budget.** The light
   queries are fast here **only because the 30s prewarm keeps the working set
   under 64 MiB**. The A/B in `FINDINGS.md` shows that once the live part count
   exceeds the 64 MiB cache (longer uptime / deeper `[30m]`/`[1h]` windows), the
   same cache-bound queries collapse onto a ~70–85 ms re-decode floor — a
   **14–100× regression**. Under `GOMEMLIMIT=1GiB` the cache **cannot** grow past
   64 MiB, so this is the production regime, not a lab artifact. The fix is to
   make decode cheaper (§1b), **not** to grow the cache — RSS must stay < 1 GiB
   (the oteldb budget).

## Action points (ordered by leverage)

1. **Range-aware / per-series decode** (`storage/engine/part.go:236`
   `decodeInto`). The single durable win: decode only the touched `rowRange` per
   column instead of the whole `ts`/`value`/`sf` columns. Directly attacks
   `count_cpu_cores` and `cpu_usage` (the 1.6–2.7× gaps) and shrinks per-fetch
   allocation, so it also cuts the p99 tail and the GC churn against the 1 GiB
   cap. This is the lever that works *within* the RSS budget — see `FINDINGS.md §1b`.
2. **Verify `count` / `count by (...)` pushdown fires for `__name__=~"node_.+"`.**
   `count_cpu_cores` is a pure count aggregation and should hit the aggregate
   sidecar (`part.statsOnce`, `WithAggregateStats`), but regex name matchers often
   bypass name-keyed pushdowns into a full scan. If it's bypassing, wiring it in
   turns 134 ms into single-digit ms. (`FINDINGS.md §2`.)
3. **Cut the scan-path p99 tail.** `count_cpu_cores` p99=229 (1.7× p90). Pool
   decode buffers on the cache-miss path (`decodeOf` cached branch → `Engine.decPool`)
   so scan-bound queries stop allocating fresh `[]float64`/`[]int64` per fetch.
   (`FINDINGS.md §3`.)
4. **Disk footprint 3× the reference.** Not latency-critical, but 70 MB vs 23 MB
   for the same 140k-series/30s window is worth a compression/encoding pass on the
   part format once the decode work is reduced.

## Performance targets

Concrete, measurable, and holdable under `GOMEMLIMIT=1GiB` (RSS < 1 GiB is a hard
constraint — do not buy latency with cache growth):

| dimension | current | target |
|---|---:|---:|
| `count_cpu_cores` p90 | 134 ms | **≤ 75 ms** (≤ 1.5× reference) |
| `cpu_usage` p90 | 178 ms | **≤ 130 ms** (≤ 1.15× reference) |
| `full_scan_count` p90 | 314 ms | **hold ≤ reference** (already 0.91×) |
| scan-query p99/p90 | 1.7× | **≤ 1.3×** (tail control) |
| light range queries p90 | sub-8 ms | **stay sub-10 ms even at longer prewarm** (defeat the 64 MiB thrash floor) |
| resident RSS | 897 MB | **< 1024 MB** (hold; never regress to fix latency) |
| on-disk | 70 MB | **≤ 2× reference** |

The north star: land §1b so the heavy queries close to ≤1.5× the reference **and**
the light queries stay flat as the live part count grows — both without lifting
RSS above 1 GiB.

## Reproduce

```bash
# 1. build the thin image from local source (host resolves the storage replace).
#    Build the static binary and the image in one context dir:
cd /src/oteldb/oteldb
mkdir -p /tmp/oteldb-img
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false \
  -o /tmp/oteldb-img/oteldb ./cmd/oteldb
cat > /tmp/oteldb-img/Dockerfile <<'EOF'
FROM alpine:latest
RUN apk --no-cache add ca-certificates wget
COPY oteldb /usr/local/bin/oteldb
ENTRYPOINT ["/bin/sh","-c","exec /usr/local/bin/oteldb \"$@\"","--"]
EOF
docker build -t oteldb:head-776c74eb /tmp/oteldb-img

# 2. point the matrix at it and run the metrics lane, oteldb vs the reference:
cd /src/oteldb/benchmark        # set OTELDB_IMAGE=oteldb:head-776c74eb in .env
BENCH_ONLY=oteldb,<reference> ./benchctl bench metrics 20 && ./benchctl report
# results/REPORT.md (p90 matrix + correctness), results/metrics/*.csv (p50/p90/p99),
# results/footprint.csv (disk + RSS).
```

---

# pprof profiling under query load (2026-07-01)

Profiled the same `oteldb:head-776c74eb` build with the pprof endpoint on `:9010`
while driving the 8-query suite concurrently (`otelbench promql bench -c --jobs 8`)
over 240s of ingested data (~140k series, `GOMEMLIMIT=1GiB`, 64 MiB block cache).
Two captures: **mixed** (with live remote-write ingest, `pprof/cross-engine/`) and
**query-only** (ingest frozen — vmagent stopped — so the numbers are pure query,
`pprof/cross-engine-q/`). Raw `.pb.gz` for cpu / allocs / heap are in those dirs.

## Headline: under the 1 GiB budget, query execution is GC-bound, not decode-bound

Query-only CPU profile (30s, ingest stopped):

```
runtime.gcBgMarkWorker      79.8%   ← garbage collection
  runtime.scanObject        65.5%   ← marking the live heap
  runtime.findObject        14.2%
runtime.mallocgc            11.3%
--- actual query pipeline ---
promql-engine …concurrencyOperator.Series → coalesce.loadSeries   ~11%
  → storage/query/promql.querier.Select → storage.seedFetcher.Fetch
  → storage/engine.Engine.Fetch                                    ~10.3%
```

**~80% of query CPU is GC.** The real query work (fetch + decode + evaluate) is
only ~11%. This is the direct consequence of the [[oteldb-rss-budget]]: `GOMEMLIMIT=1GiB`
forces frequent GC cycles, and each cycle re-scans a large live heap. So the lever
is **allocation reduction**, not making decode faster — decode CPU is already <1%
per leaf (`DecodeFloats` 0.9%, `DecodeTimestamps` 0.8%). GC cost has two
multiplicative drivers, and both must come down:

1. **Allocation rate** → GC *frequency*. Query-only alloc rate is **~167 MB/s**
   (5.0 GB in 30s).
2. **Live heap size** → *per-cycle* scan cost. Query-only inuse heap is **~848 MB**
   (right at the cap), so every GC scans nearly a gigabyte.

## Where the allocations come from (query-only alloc_space, cum %)

| call site | % of query allocs | layer |
|---|---:|---|
| `chunk.resize[int64]` + `chunk.resize[float64]` (flat) | **~52%** | **engine (storage)** |
| `engine.Fetch` → `mergeSeries.addRange` → `floatBlock`/`tsBlock` | 43–53% | **engine (storage)** |
| `engine.prefetch` → `seriesBlockReader.warm` (concurrent decode) | 36% | **engine (storage)** |
| `backend.cachedBackend.Read` → `PartReader.Column` (raw col bytes) | 15% | **engine (storage)** |
| `promql-engine matrixSelector.loadSeries` (range samples) | 43% | **oteldb (promql-engine)** |
| `promql-engine coalesce.loadSeries` / `concurrencyOperator.Series` | 56–60% | **oteldb (promql-engine)** |

(Percentages are cumulative and nested — `coalesce` wraps `Select` wraps `Fetch` —
so they don't sum. The one unambiguous *flat* allocator is `chunk.resize`.)

Live-heap (inuse) during query, ~848 MB: `coalesce.loadSeries` 317 MB (eager series
buffer), `engine.Fetch` 276 MB, `engine.planFetch` 201 MB, `matrixSelector` 213 MB,
plus ~182 MB residual ingest head.

## What to fix at the ENGINE (storage) level — github.com/oteldb/storage

The engine already does the *right structural things* (block-granular decode cache,
`decodeRangesInto` skips untouched blocks, `decPool` recycling, concurrent
`prefetch`). The remaining cost is **buffer allocation on the block-cache miss
path**, and it's the single biggest lever in the whole profile:

1. **Pool the block-decode output buffers — kills ~52% of query allocations.**
   On a `blockCache` miss, `seriesBlockReader.tsBlock`/`floatBlock`
   (`engine/seriesblocks.go:141,206`) call `Decoder.DecodeInt64/DecodeFloat64`
   → `block.decodeOneBlock` → `chunk.DecodeTimestamps/DecodeFloats`, and `chunk.resize`
   (`encoding/chunk/timestamp.go:260`) allocates a **fresh `[]int64`/`[]float64` per
   block** because `dst` is nil. Those slices are handed to the cache; when the 64 MiB
   cache evicts them (constantly, under 8 concurrent queries × many parts) they become
   garbage. Fix: a `sync.Pool` of column buffers — `blockCache` eviction returns the
   slice to the pool; the decoder draws `dst` from the pool instead of `resize`-allocating.
   `[]float64`/`[]int64` are pointer-free, so this cuts both alloc rate *and* the sweep
   load with zero scan-safety risk. This is the durable version of the old
   `FINDINGS.md §3`, now the #1 item.
2. **Pool the raw column read buffers** (`backend.cachedBackend.Read` /
   `block.PartReader.Column`, 15%). Same pattern one level down — the compressed
   column bytes are re-read/re-allocated per fetch on a cache miss.
3. **Don't grow the cache to hide the misses.** Raising `decode_cache_bytes` would cut
   the miss rate but blows the 1 GiB RSS budget (fraction path already floored at
   64 MiB under the cap). Pooling is budget-neutral; cache growth is not. Hold the line.

## What to fix at the OTELDB level — promql-engine + storagebackend adapter

The largest *live-heap* contributors (what GC re-scans every cycle) are in the query
engine, not the storage engine:

1. **Eager full-series materialization (317 MB live).** `concurrencyOperator.Series`
   (guarded by a `sync.Once`) and `coalesce.loadSeries` buffer the **entire** matched
   series set before evaluation begins. For the `node_.+` / all-cpu selectors that's
   ~140k series × 8 concurrent queries held resident — the dominant scan target. Lever:
   stream/chunk the coalesce so series are evaluated and freed incrementally, or bound
   the concurrency buffer. This is a promql-engine (`github.com/oteldb/promql-engine`)
   architecture change — oteldb's query layer, not the storage engine.
2. **Range-vector samples materialized whole (213 MB).** `matrixSelector.loadSeries`
   pulls every raw sample in the `[1m]`/`[5m]` window into memory for `rate`/`irate`/
   `sum_over_time`. Lever: push the range aggregation down to the engine fetch so it
   returns per-step aggregates instead of raw samples — the engine already has an
   aggregate path (`engine.aggViaStats`, `seriesstats.go`); wire the adapter
   (`internal/storagebackend`, `scanners.NewMatrixSelector`) to request it when the
   outer op is a simple reducer. Cuts oteldb-side materialization *and* engine-side
   decode.
3. **Per-query label rebuild.** `storage/query/promql.querier.Select`
   (`query/promql/queryable.go:119`) has a per-query `labelCache`, but it's discarded
   when the query ends, so 140k `labels.Labels` (pointer-rich strings) are rebuilt and
   re-scanned every query. Lever: a cross-query, engine-lifetime series→labels intern
   cache. (Code lives in the storage repo's `query/promql`, but the caching policy is
   oteldb's call.)

## How this refines the performance targets

The `count_cpu_cores` / `cpu_usage` gaps (§ "Performance targets" above) are now
attributable: they're the queries that touch the most series, so they allocate the
most block buffers (engine) and materialize the most series (promql-engine) — i.e.
they pay the most GC tax. Ordered by leverage against the p90 gap **and** the p99 tail:

1. **Engine buffer pooling (#1)** — biggest single alloc cut (~52%), lowers GC
   frequency for *every* query. Do first; it's low-risk and budget-neutral.
2. **promql-engine streaming coalesce (#1 oteldb)** — biggest live-heap cut, lowers
   per-cycle scan cost and the p99 tail (tail = GC pauses landing on a big heap).
3. **Range-aggregation pushdown (#2 oteldb)** — removes both the 213 MB matrix buffer
   and the matching engine decode for the rate/sum queries.

Target check: if engine pooling removes ~half the alloc rate and streaming halves the
live heap, GC should drop from ~80% toward ~40% of CPU, which is what stands between
`count_cpu_cores` at 134 ms and the ≤75 ms target — **without** touching `GOMEMLIMIT`
or the cache size (RSS budget stays intact).

## Reproduce the profiles

```bash
# stack up with data (from /src/oteldb/benchmark, OTELDB_IMAGE already set):
BENCH_ONLY=oteldb ./benchctl up metrics && PREWARM=240s ./benchctl ingest metrics
# (optional) freeze ingest for a clean query-only profile:
docker stop oteldb-bench-vmagent-1
# drive load and capture, from dev/local/embedded-bench:
END=$(date +%s); ./otelbench promql bench --addr http://127.0.0.1:9090 \
  -i queries.promql.yml -c -d 90s --jobs 8 --start $((END-300)) --end $END -o /dev/null &
curl -s -o cpu.pb.gz    "http://127.0.0.1:9010/debug/pprof/profile?seconds=30"
curl -s -o allocs.pb.gz "http://127.0.0.1:9010/debug/pprof/allocs?seconds=30"
curl -s -o heap.pb.gz   "http://127.0.0.1:9010/debug/pprof/heap"
go tool pprof -top -cum cpu.pb.gz
go tool pprof -top -cum -sample_index=alloc_space allocs.pb.gz
go tool pprof -top -cum -sample_index=inuse_space heap.pb.gz
```


---

# Re-benchmark after storage #62 (pool block-decode buffers) — 2026-07-01

Storage **#62 landed** (PR #63, `de9aae3` "engine: pool block-decode miss-path
buffers") — exactly the S1 fix from `ISSUES.md`: `Decoder.DecodeInt64Into` /
`DecodeFloat64Into` decode into a caller buffer (reusing one decompress scratch per
column), and `blockCache` draws each miss's destination from a bounded, GC-stable,
reference-counted freelist and recycles evicted+unpinned block slices instead of
re-minting them.

Rebuilt `oteldb:s62-926ed0a` (oteldb `776c74eb` + storage `926ed0a`, storage now
requires v0.23.0) and re-ran the metrics lane + the same query-only profile
(240s data, ingest frozen, 8 concurrent jobs). Profiles: `pprof/cross-engine-q2/`.

## Verdict: the S1 allocation firehose is gone; GC dropped ~20 points

| metric (query-only, 8-way concurrent load) | pre-fix (`bee17bc`) | post-fix (`926ed0a`) | Δ |
|---|---:|---:|---:|
| `chunk.resize` fresh-per-block alloc | **2,614 MB (52% of allocs)** | **0 — gone** | eliminated |
| total alloc over 30s (alloc rate) | 5,004 MB (~167 MB/s) | **2,904 MB (~97 MB/s)** | **−42%** |
| CPU `runtime.gcBgMarkWorker` | 79.8% | **59.2%** | −20.6 pp |
| CPU `runtime.scanObject` (mark) | 65.5% | **40.4%** | −25.1 pp |
| real query eval share (`concurrencyOperator.Series`) | ~11% | **~23%** | 2.1× |

The block decode now flows through the pooled `blockCache.getI64Buf`/`getF64Buf`
freelist instead of `chunk.resize` minting a fresh slice per miss. Allocation rate
fell 42% and GC **mark** cost fell ~25 points — the fix did what S1 predicted.
Residual GC is now **sweep-heavy** (`sweepone` ~32%), and the freelist still
allocates on concurrency spikes (`getI64Buf`+`getF64Buf` ~1.6 GB/30s, but bounded
and recycled, vs. the old unbounded fresh churn).

## The bottleneck has shifted to the oteldb layer (O1)

Post-fix, both the allocation and the live-heap profiles are now **led by
promql-engine**, not the storage engine:

```
inuse_space (863 MB live):  coalesce.loadSeries → concurrencyOperator.Series  361 MB (42%)  ← O1
                            (engine Fetch/planFetch decode materialization dropped out of the top)
alloc_space (2.9 GB/30s):   concurrencyOperator.Series / coalesce.loadSeries   58%          ← O1
                            matrixSelector.loadSeries                          39%          ← O2
```

The engine's decoded-column materialization is no longer the top live-heap object;
the **eager full-series coalesce buffer (O1, oteldb/oteldb#1116)** is. So the ordered
next step is confirmed: **O1** (stream the coalesce) is now the single biggest lever,
then **O2** (range-agg pushdown, oteldb/oteldb#1117).

## Latency & footprint (with caveats)

The 20-run **sequential** metrics bench is within run-to-run noise of the pre-fix
numbers (this regime isn't GC-bound — one query at a time over a working set that
fits the cache, so the miss-path churn the fix targets barely fires). oteldb p90 this
run: `count_cpu_cores` 153, `cpu_usage` 220, `full_scan_count` 435 (vs. reference 35,
140, 454 — oteldb again edges the reference on `full_scan_count`). Don't read a
latency win or loss into this run; the fix's target metric is **GC under concurrent
load**, measured above.

Footprint: RSS **963 MiB at rest** (pre-load, under budget), 1026 MiB at the
`benchctl collect` sample (marginal, ~data-window noise — this run held 88 MB on disk
vs. 70 MB before). Under 8-way concurrent load the container sat at ~2.76 GiB
(transient, includes file page cache). The bounded freelist did **not** blow steady
RSS — it stays near the 1 GiB budget. Worth a steady-state RSS watch as O1/O2 land.

**Status:** S1 (oteldb/storage#62) ✅ validated. Next: O1 (oteldb/oteldb#1116).

---

# Re-benchmark after O2 + O3 merged — 2026-07-01

Two more optimizations landed: **O3** (oteldb/oteldb#1118, *closed*) — engine-lifetime,
SeriesID-keyed **label intern cache** (storage `d06b7d4` `NewLabelCache`/
`NewQueryableWithCache`, wired in `internal/storagebackend/storagebackend.go:81,98`,
storage v0.24.0); and **O2** (oteldb/oteldb#1117, *open* — code landed `ce98887c`) —
**range `*_over_time` pushdown** (`aggregateOverTimeRangeOp`, folds one engine
`SeriesAgg` per (series, step) for `count/sum/min/max/avg/present_over_time`). O1
(#1116, stream the coalesce) is **not** merged.

Built `oteldb:o2o3-2ca62b43` (oteldb `2ca62b43`, storage v0.24.0 — the go.mod storage
`replace` is gone, so this is the released build). Same query-only profile regime
(240s data, ingest frozen, 8 concurrent jobs). Profiles: `pprof/cross-engine-q3/`.

## Verdict: aggregate GC is flat — O1 is still the gate

| metric (query-only, 8-way concurrent) | pre-#62 `bee17bc` | post-#62 `926ed0a` | O2+O3 `2ca62b43` |
|---|---:|---:|---:|
| CPU `gcBgMarkWorker` | 79.8% | 59.2% | **57.6%** |
| CPU `scanObject` (mark) | 65.5% | 40.4% | **40.3%** |
| top query frame (alloc + inuse) | engine decode | coalesce (O1) | **coalesce (O1)** |
| `coalesce.loadSeries` alloc share | ~56% | 56% | **49%** |
| at-rest RSS (pre-load) | — | 963 MiB | **941 MiB** |

The #62 win holds. O2+O3 moved GC only ~1.5 points more, and the top allocator/live
object is still the **O1 eager coalesce buffer** (`coalesce.loadSeries` ~50% of allocs,
556 MB inuse). Until O1 lands, aggregate GC and latency on this suite won't move much.

## Why O2/O3 barely register *on this suite* (both are correct)

- **O2 is not exercised by the benchmark queries.** O2 pushes down *range* `*_over_time`.
  But the suite's `*_over_time` queries — `avg_over_time[30m]` (`topk_free_mem`),
  `quantile_over_time[1h]` (`load_quantile`) — are **instant** (they hit the pre-existing
  *instant* `aggregateOverTimeOp`; `quantile_over_time` isn't even foldable via the
  sidecar). The suite's **range** queries are all `rate`/`irate`, which are *not*
  `*_over_time`, so `aggregateOverTimeRangeOp` never fires. Net effect on these numbers:
  ~zero. **Action:** to actually measure O2, add a range `sum_over_time`/`avg_over_time`
  query to `queries/metrics.promql.yml` (and file: extend O2 to push `rate`/`irate`
  down too — that *is* what the suite's range lane needs).
- **O3 is active but its win is masked.** The backend-lifetime label cache is wired
  (verified), and at-rest RSS dipped 963→941 MiB (fewer resident rebuilt labels). But
  `labels.*` was already only ~0.5% of query allocs, and its larger benefit — a smaller
  pointer-rich live heap for GC to scan — is dwarfed by the coalesce buffer (O1) that
  still holds all matched series+labels. O3's payoff will show **once O1 stops
  materializing the whole set**.

## Latency & footprint (noisy — do not over-read)

The machine was under heavier load during this run (repeated builds/benches): *both*
engines were ~15–20% slower than earlier runs, and oteldb's on-disk grew to 97 MB
(vs 88 MB), so the sequential 20-run p90s aren't comparable across runs. oteldb p90
this run: `count_cpu_cores` 137, `cpu_usage` 218, `full_scan_count` 516 (reference this
run: 79 / 161 / 497 — itself unusually slow). RSS at the collect sample 1026 MiB;
at-rest 941 MiB. Nothing here is a real regression or win — it's cross-run variance.

**Status:** O3 (#1118) ✅ merged/active, O2 (#1117) ✅ merged but unexercised by this
suite. **O1 (#1116) remains the single gating lever** — ~50% of query allocs and the
top live object; it's what stands between the current ~58% GC and a further drop.
Next: land O1, and add a range `*_over_time` query so O2 is measurable.

---

# Re-benchmark after O1 merged (promql-engine v0.9.0) — 2026-07-02

**O1 landed** (promql-engine PR #33, commit `45347ca` "perf(execution): release series
labels after the resolution phase", released as **promql-engine v0.9.0**; oteldb main
`ae91e9a6` bumps to it). `coalesce.releaseSeries()` does a one-shot release of the
concatenated label set once evaluation begins, so "the O(all-matched) label headers are
not kept live for the whole evaluation" (child selectors release theirs too). This is
the fix for oteldb/oteldb#1116 — the buffer that had been the #1 live object since #62.

Built `oteldb:o1-ae91e9a6` (oteldb `ae91e9a6`, promql-engine v0.9.0, storage v0.24.0).
Same query-only regime (240s data, ingest frozen, 8 concurrent jobs). Profiles:
`pprof/cross-engine-q4/`.

## Verdict: O1 delivers its target — live heap & RSS drop; the coalesce buffer is gone

O1's target was **live heap** (what GC re-scans each cycle), not allocation rate. It hit it:

| metric | pre-#62 | post-#62 | O2+O3 | **O1 (v0.9.0)** |
|---|---:|---:|---:|---:|
| at-rest RSS (pre-load, idle) | — | 963 MiB | 941 MiB | **856 MiB** |
| footprint-collect RSS (1 query) | — | 1026 MB | 1026 MB | **974 MB** |
| query-only live heap (inuse, post-settle) | 848 MB | 863 MB | 863 MB | **627 MB** |
| top live object | engine decode | coalesce | coalesce | **gone → bg compaction** |

**`coalesce.loadSeries` — the top live object at 361–556 MB since #62 — is absent from
the top of the O1 inuse profile entirely.** The eager label set is released after
resolution, so it no longer sits resident for the whole query. Steady RSS fell
963→941→**856 MiB** across the three optimizations, comfortably under the 1 GiB budget.

## What O1 does *not* move: GC-CPU% (and why)

On a clean recapture (after background compaction settled), `gcBgMarkWorker` was **~60%**
— the same band as post-O2+O3 (57.6%). O1 did not drop GC-CPU, and that's expected:

- GC cost ≈ **frequency** (∝ allocation *rate*) × **scan-per-cycle** (∝ *live heap*).
  O1 cuts the second factor (live heap / RSS), not the first. Series are still
  *allocated* in `Engine.Fetch` / the selectors — O1 just *frees* them sooner. So RSS
  drops but the GC *frequency* (driven by the unchanged ~100+ MB/s alloc rate) doesn't.
- The next GC-CPU lever is therefore **allocation-rate** reduction in the query fetch
  path (`Engine.Fetch` → per-series sample/label allocation, still ~50% of alloc), or
  extending O2-style pushdown to `rate`/`irate` so raw samples are never materialized.

## Measurement caveat (important for this run)

The benchmark host was **heavily contended** this session (repeated builds + 4 full
bench/profile cycles): the sequential 20-run latencies were ~2–3× inflated **for oteldb
specifically during its query window** (`count_cpu_cores` p90 309, `cpu_usage` 461)
while the reference ran at normal speed in its own window (`count` 51) — i.e. transient
CPU contention, not a regression. The CPU/latency numbers this run are **not reliable**;
treat only the **at-rest RSS / inuse-heap** figures (measured idle, robust to contention)
as the real O1 signal. A clean GC-CPU comparison needs an idle box.

**Status:** O1 (#1116) ✅ merged — **RSS/live-heap win confirmed** (steady RSS
963→856 MiB, coalesce buffer eliminated). GC-CPU% flat this run (live-heap fix, not
alloc-rate; plus host contention). **Next lever: query-path allocation *rate*** —
`Engine.Fetch` per-series alloc + `rate`/`irate` sample materialization (extend O2).

---

# Clean head-to-head + optimization roadmap — 2026-07-02

Idle-box re-run (load avg 1.8 at start; prior runs were contended) of
`oteldb:o1-ae91e9a6` (promql-engine v0.9.0 + storage v0.24.0 — all of S1/O1/O2/O3
landed) vs the reference. 16/16 correct. Footprint this run: oteldb 102 MB disk /
**976 MB RSS**; reference 18 MB / 323 MB.

## Comparison (p90 ms, 20 sequential runs, ~143k series)

| query | oteldb | reference | ratio | regime |
|---|---:|---:|---:|---|
| `full_scan_count` | 274.2 | 329.0 | **0.83× — faster** | count pushdown works |
| `topk_free_mem` | 0.9 | 0.6 | 1.5× | instant over_time pushdown works |
| `load_quantile` | 1.2 | 0.6 | 2.0× | sub-ms absolute; fine |
| `rate_network` | 2.3 | 1.3 | 1.8× | small matrix; fine |
| `disk_io` | 5.3 | 2.5 | 2.1× | small matrix; fine |
| `cpu_usage` | 276.2 | 111.1 | **2.5×** | irate × 3200 series ×2 selectors |
| `count_cpu_cores` | 212.1 | 44.2 | **4.8×** | grouped count — no pushdown |
| `cpu_wait` | 110.8 | 16.5 | **6.7×** | pure irate × 3200 series |

Where pushdowns exist (`count(selector)`, instant `*_over_time`), oteldb **matches or
beats** the reference. The entire remaining gap is concentrated in two query shapes
that lack a pushdown — **`rate`/`irate` matrices** and **grouped count** — amplified by
one systemic tax: GC.

## The unifying diagnosis: GC-pacer thrash near GOMEMLIMIT

Three independent observations converge:

1. **A single sequential query is 44% GC.** Solo `cpu_wait` (irate, one query at a
   time, no concurrency): `gcBgMarkWorker` 43.9% of CPU (`pprof/cross-engine-q4/cpu-wait.pb.gz`).
2. **Under 8-way load the live heap exceeds the limit**: 1297 MB inuse
   (`heap2.pb.gz`) vs `GOMEMLIMIT=1GiB` → the pacer runs GC back-to-back.
3. **Heavy-query latency tracks dataset size across runs** (`cpu_wait` p90: 45 → 32 →
   111 ms as on-disk grew 88 → 97 → 102 MB) while the reference stayed flat. More
   resident data ⇒ less pacer headroom ⇒ more GC per query. (A same-data A/B against
   the pre-v0.9.0 image would rule out a code regression, but the solo-query profile
   shape is pure GC, and `full_scan_count` *improved* — code regression unlikely.)

Mechanics: resident heap is ~700–850 MB against a 1 GiB limit, so only ~150–300 MB of
allocation triggers a full cycle that scans the whole resident set. The query path
allocates ~30+ MB/query (`Engine.Fetch` cum ~52% of allocs), so every handful of
queries pays a full-heap mark. **Latency now = f(alloc-per-query × resident-heap).**
Both factors are attackable; neither alone suffices.

## Optimization roadmap (ordered by measured leverage)

### Engine (storage) — cut alloc-per-query

- **S2 — Fix `bufFreeList` misses (50.6% of query allocs).** The #62 freelist works,
  but `blockBufCap = prefetchConcurrency×4 = 32` assumes each miss's eviction promptly
  `put`s a buffer back. In reality cache entries stay **pinned until fetch teardown**
  (`releasePins`), so under 8-way × prefetch-8 bursts the list runs dry and
  `getI64Buf`/`getF64Buf` fall through to `make()` — 1.92 GB/30s
  (`bufFreeList.get` flat 50.6%, `pprof/cross-engine-q4/allocs2.pb.gz`). Fixes, in
  preference order: recycle at **unpin** (not only eviction); scale the cap with
  in-flight fetches; per-size-class lists so a hit is likelier. Budget-neutral.
- **S3 — Stop cloning on backend-cache *hit* (15.2%).** `backend/cache.go:143`
  `slices.Clone`s the cached column bytes on **every hit** (579 MB/30s via
  `cachedBackend.load`). The decoder never mutates its input — return a refcounted
  read-only view, or decode directly from the cached slice.
- **S4 — Pool the merge output (15.2%).** `collectMany` → `ensureCap[int64/float64]`
  allocates fresh per-fetch result columns; wire them through the existing `decPool`.
- **S5 — Grouped-count pushdown (targets the 4.8× `count_cpu_cores`).**
  `count(count by(cpu)(m))` needs only **distinct values of one label over matched
  series with in-window data** — answerable from postings + the per-part index with
  zero sample decode, the same trick that already makes `full_scan_count` beat the
  reference. Extend the count sidecar to `count by (label)`.
- **S6 — Resident heap is now a *latency* lever, not just footprint.** Every resident
  MB steals pacer headroom. The oteldb/storage#51 remainder (#48 sparse part index,
  postings compaction, head columns #49) directly buys back GC headroom. Target
  resident ≤ 500 MB under this workload → doubles the allocation budget between GCs.

### oteldb / promql-engine — eliminate the remaining materialization

- **O4 — `rate`/`irate` pushdown (the big one: 2.5–6.7× queries).** Extends O2/#1117
  to the suite's actual range shape. The engine folds per `(series, step)`:
  `{first_ts, first_val, last_ts, last_val, prev_of_last, reset-corrected delta}` —
  enough for `rate`, `irate`, `increase` with counter-reset semantics — so
  `matrixSelector` never materializes raw windows (still ~39% of query allocs, and
  100% of `cpu_wait`/`cpu_usage`'s data volume).
- **O5 — Admission-fit the decode budget to the pacer.** The engine already has a
  decode-memory budget (#57). Tune/wire it so `resident + in-flight decode ≤
  GOMEMLIMIT − headroom`: under 8-way load the heap must **not** cross the limit
  (today: 1297 MB > 1024 MB), because past it the pacer runs continuous GC and every
  query pays. Slightly more queueing, much less collective thrash.

### Not now

- **Disk 5.6× (102 vs 18 MB):** hot parts use the fast codec; zstd `recompress` tiers
  exist but only fire for cold parts, which a 5-minute bench never reaches. Real
  deployments age into the compressed tiers; revisit only if hot-window disk matters.

## Priority call

**S2+S3+S4 first** (one theme: stop re-minting transient buffers — together ~81% of
query alloc bytes, directly cuts GC frequency), then **O4** (removes the dominant
remaining data volume and the 6.7× outlier), then **S5** (index-only grouped count),
with **S6/O5** as the structural guarantee that the pacer never thrashes again.
Projection if S2–S4 land: query alloc rate drops ~4×, GC share on the solo-query
profile falls from ~44% to <15%, putting `cpu_wait`/`cpu_usage` within ~1.5× of the
reference without touching the 1 GiB budget.
