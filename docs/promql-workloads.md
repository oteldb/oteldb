# PromQL workloads: memory and parallelism

What four typical query shapes cost in `internal/scarecrow` today, how VictoriaMetrics, the
Thanos fork, Mimir and ClickHouse handle the same shapes, and what `oteldb/storage` and the
engine are each missing.

Companion to `docs/promql-engine.md`. That document argues a design; this one measures it.

## The four archetypes

| | Shape | Query | Series | Steps |
|---|---|---|---|---|
| **A** | recording rule | `sum by (job) (rate(m[5m]))` | 1,000,000 | 1 |
| **B** | long history, few series | `m` | 10 | 172,801 (30d @ 15s) |
| **C** | small instant | `m{instance="x"}` | 5 | 1 |
| **D** | DC-wide historical aggregate | `avg(avg_over_time(m[1h]))` | 100,000 | 8,641 (30d @ 5m) |

They are chosen because they load different axes: A loads cardinality, B loads the step axis, C
loads neither, D loads both and then collapses the first.

## Method

`TestMemoryArchetypes` (`internal/scarecrow/memprofile_test.go`, `SCARECROW_MEM=1`) runs each
shape against a synthetic scanner that reuses one sample buffer per series, so the scanner is
`O(1)` in cardinality and anything that grows is the engine's. Three numbers per run:

- **live** — heap still reachable after a GC with the result held. What the query *retains*.
- **peak** — largest heap sampled at 1 kHz during the run. Includes garbage not yet collected.
- **churn** — total allocated. A proxy for GC pressure, not for footprint.

Sizes are scaled down where a faithful run would be too slow, and each case is run at two sizes
along the axis in question so the *shape* is visible rather than asserted. Figures are one
machine, one Go version; the ratios are the point, not the absolutes.

## Measured — scarecrow today

| Case | Series | Steps | live | peak | churn | time |
|---|---:|---:|---:|---:|---:|---:|
| A | 100,000 | 1 | 1.7 MB | 35.9 MB | 136 MB | 0.17 s |
| A | 1,000,000 | 1 | 1.8 MB | **404.5 MB** | 1,417 MB | 1.88 s |
| B | 10 | 5,761 | 2.6 MB | — | 1.7 MB | 0.00 s |
| B | 10 | 172,801 | 28.1 MB | 40.2 MB | 47.8 MB | 0.03 s |
| C | 5 | 1 | — | — | — | 0.00 s |
| D | 1,000 | 8,641 | 1.8 MB | 5.8 MB | 865 MB | 0.82 s |
| D | 5,000 | 8,641 | 1.9 MB | **7.0 MB** | 4,320 MB | 4.03 s |
| D2 (no fan-in) | 1,000 | 8,641 | 134.7 MB | 220.7 MB | 997 MB | 0.74 s |
| D2 (no fan-in) | 5,000 | 8,641 | 666.4 MB | 1,065.8 MB | 4,984 MB | 3.65 s |
| E (1:1 join) | 5,000 | 241 | 21.9 MB | 37.5 MB | 60 MB | 0.05 s |
| E (1:1 join) | 5,000 | 8,641 | 666.5 MB | **1,218.8 MB** | 1,743 MB | 0.96 s |

**D is the design working exactly as claimed.** 5,000 series × 8,641 steps folded into one group
costs **7 MB**, and it is *flat in cardinality* — 1,000 → 5,000 series moves peak from 5.8 MB to
7.0 MB. The `O(groups × steps)` cost model holds where it matters most.

**D2 is the same query without the collapsing aggregation**, and it costs 150× more. That is not
a defect: output cardinality equals input, so the result itself is `5,000 × 8,641 × 16 B = 691 MB`.
Every engine here pays it. It is worth stating because it means *the aggregation, not the engine,
is what makes D cheap.*

**E confirms §4.6's exception.** A one-to-one join buffers its build side, so 5,000 series ×
8,641 steps peaks at 1.2 GB — result plus a full `5,000 × 8,641 × 8 B = 345 MB` build side.

## Where scarecrow's memory actually goes

Three distinct costs, only one of which the design doc currently accounts for.

**1. The result.** `O(output series × steps × 16 B)`. Unavoidable without streaming the HTTP
response, which no engine here does. B's 28.1 MB is *entirely* this (`10 × 172,801 × 16 B =
27.6 MB`), and the Thanos fork's figure for the same query is identical. For B, the engine is not
the cost — the answer is.

**2. The accumulator.** `O(groups × steps)`. This is what the design optimizes and it works (D).

**3. The schema — `O(matched series)`, and this one is not in the doc.** Plan-time schema
resolution (§3.3) holds every matched series' label set for the query's lifetime. Measured
directly: **69 MB per 1,000,000 series** (~72 B/series including the memoized hashes). A's 404 MB
peak is that schema plus uncollected garbage from per-series label construction.

That third cost matters because `docs/promql-engine.md` §2 claims series-major means "nothing is
proportional to the matched series set". **That is true of the sample path and false of the schema
path.** The engine removes oteldb#1117 (raw samples in every window) but keeps a smaller version
of oteldb#1116 (label sets for the matched set) — 69 MB where the fork's `coalesce.loadSeries`
holds an estimated ~190 MB for the same 1M series, because the fork retains richer per-series
state. Better by ~3×, not eliminated. §4.6 should say so.

## Parallelism — what each engine does

| | Unit of parallel work | Degree | Step axis |
|---|---|---:|---|
| **scarecrow** | binop sides only | ≤2 per binop, GOMAXPROCS slots process-wide | sequential |
| **VictoriaMetrics** | one series → one rolled-up series | `min(GOMAXPROCS, 32)` | sequential |
| **Thanos fork** | contiguous shard of the series list | `max(GOMAXPROCS/2, 1)` | sequential, ≤64 steps in flight |
| **Mimir** | AST shard (`__query_shard__`) + 24h interval split | 16 shards × 30 splits, across processes | sequential per shard |
| **ClickHouse** | block of ≤65,536 rows from a part/granule | `max_threads` = cores | n/a |

**Every engine surveyed parallelizes over series and none parallelizes over steps.** That is not
a coincidence: steps are cheap and independent, series carry the I/O. It also means scarecrow's
step-axis-sequential design is not the gap — the missing series-axis parallelism is.

**scarecrow is effectively single-threaded for A, B, C and D.** `Concurrent` only wraps the two
sides of a vector binop, so a query without one runs the whole scan-fold-aggregate pipeline on one
goroutine. A takes 1.9 s for 1M series and D takes 4.0 s for 5,000 × 8,641 with 7 MB of memory and
one core busy. Both are the shapes every other engine fans out.

The measurements say which fan-out to build: **D's churn is 4.3 GB for 7 MB of live heap**, so it
is allocation-bound in the fold, and it is embarrassingly parallel across series because the
accumulator is a commutative fold. That is M4's series sharding, and it is worth more than any
remaining memory work.

## What `oteldb/storage` is missing

Findings from reading `github.com/oteldb/storage` at `v0.33.0-4-gf13d61f`. These matter because
M5's pushdowns are dormant until a columnar `Scanner` exists over this seam. **Items 1 and 7 were
fixed in storage v0.34.0**; they are kept here because they document why the seam needed changing.

**1. `engine.Fetch` was eager, and this was bigger than storage#208.** ~~`engine.Fetch`
(`engine/engine.go:401-468`) builds a `[]*fetch.Batch` for *every matched series* before returning
an iterator.~~ storage#208 correctly reported that `fetch.Merge` drains and deep-copies its
children, but the base producer was already fully materializing — so fixing `Merge` alone would not
have made a single-child fetch stream. **The engine's whole `O(1)`-in-series claim rested on a seam
that did not stream.** Filed as storage#211; **fixed in v0.34.0** together with #208, which now
k-way merges the fan-out by `SeriesID` with one pending batch per child. `Fetch` gathers one
series per `Next`, so peak live heap for a fold-and-release consumer drops ~4.5× (12.5 MB vs
57.2 MB at 100k×8). Callers now owe `Close`; every oteldb call site goes through `fetch.Drain`,
which closes what it drains.

**2. `planFetch` presizes to the matched set.** It allocates a `[]signal.Series` plus two maps
sized to the matched series count (`engine.go:1367-1388`), ~200–400 B/series — 200–400 MB at
archetype A's cardinality, before any samples. The v0.34.0 streaming change explicitly leaves this
in place: the plan holds one identity and one head snapshot per series, copied under the engine
lock because a concurrent flush can move a head buffer into a part the plan never acquired. So the
residual after streaming is the plan, not the batches.

**3. The decode budget is charged by part, not by selector — deliberately.** `decodeEstimate`
(`engine.go:600-624`) reserves `part.rows() × 8 × cols` for every touched part, so archetype B
(10 series over 30 days) reserves for whole parts to produce 27.6 MB of answer. This looked like a
cost-model blind spot until the comment was read: `decodedPart` really is sized to the part's full
row count even for a sparse selector, so **the reservation matches the footprint it caps**. The
inefficiency worth chasing, if any, is the whole-part decode itself — not the estimate. No issue
filed.

**4. The series-major aggregate primitive already exists but is not exposed.**
`engine.AggregateStep`/`AggregateStepNamed` (`aggregate.go:83-163`) already returns, per series,
every step-aligned bucket in one call — folding from the per-part stats sidecar with no value
decode when the parts are contained. `Storage.AggregateMetricsNamed` only ever calls it with
`step=0`. Exposing the stepped form is a thin wrapper, and it would let M5's `AggregateScanner`
answer a whole range query per series in one call instead of one call per step — removing the
`O(series × steps)` pivot that pushdown currently pays. Filed as storage#212.

**5. But it cannot do sliding windows**, which is what archetype D needs (`[1h]` every 5m is a 12×
overlap). `bucketSeries` assumes each sample lands in exactly one bucket. Supporting overlap is
new logic in the sidecar fold, not a parameter — medium difficulty, and it is the single highest-
value storage change for D-shaped queries. Filed as storage#213.

**6. No label-ordered delivery.** Order is `signal.SeriesID` (a content hash) throughout. A merge
join needs order by a caller-chosen label subset. Inserting a sort between `head.resolve` and the
batch loop looks straightforward since identities are already snapshotted — but see the note on
`optimize_aggregation_in_order` below before deciding it is worth it.

**7. `SplitFetcher` spawns one unbounded goroutine per sub-window** (`query/scale/scale.go:50-78`,
a bare `go func` in a loop). A 30-day query split hourly launches ~720. Unrelated to PromQL, but
found while reading; filed as storage#214 and **fixed in v0.34.0**.

## What the other engines do that we should consider

**VictoriaMetrics — estimate memory before executing.** Before evaluating a rollup, VM computes
`timeseriesLen×1000 + rollupPoints×16` and acquires it from a *process-wide* semaphore sized at
25% of allowed memory (`eval.go:1845-1917`). Two things are smart here. The estimate is cheap
because series count is known after index resolution. And when the expression is an aggregation
with grouping, `timeseriesLen` collapses to `GOMAXPROCS × 1000` rather than the matched count —
the estimator *knows* the fold is streaming. That is precisely scarecrow's D case, and it is the
model to copy for M11 rather than Prometheus' `MaxSamples`.

**Mimir — MQE is the closest relative.** Series-major, one series' full timeline per `NextSeries`,
ring buffers holding only the lookback window, byte-accounting pools wired to a
`MemoryConsumptionTracker`. It reaches the same conclusions as this design independently, which is
reassuring. Note MQE explicitly abandons `MaxSamples` (hardcoded to `MaxInt`) in favour of byte
accounting — worth following.

Mimir also shows the limit of query sharding: it only shards *aggregations*, and B and D2 are not
shardable, so its 16× win applies to A and not to the shapes that actually hurt.

**ClickHouse — `optimize_aggregation_in_order`.** If the `GROUP BY` key is a prefix of the table's
sort order, ClickHouse keeps only the *current* group's state and emits it when the key changes —
no hash table at all. The PromQL analogue: if storage delivered series grouped by the `by(...)`
label subset, an aggregation would need one group's row resident rather than all of them, turning
`O(groups × steps)` into `O(steps)`.

This is the same capability as storage gap #6, and it is a better argument for it than merge
joins. But note the measurement: D's accumulator is 7 MB. Eliminating it saves 7 MB on a query
whose result is already 1.8 MB — worth almost nothing. **Ordered delivery is a CPU/allocation
optimization here, not a memory one**, and should be justified on that basis if at all.

**ClickHouse — spilling.** `max_bytes_before_external_group_by` and external sort do not transfer.
PromQL group state is `groups × steps` floats, known before execution; there is no unbounded
intermediate to spill. Building spill machinery would solve a problem this workload does not have.

**ClickHouse — the `MemoryTracker` hierarchy** (per-query → per-user → per-server, fed by every
worker thread) does transfer, and is independent of spilling. It is the enforcement mechanism M11
needs.

## Conclusions

Ranked by what the measurements support.

1. **Series-axis parallelism (M4 sharding) is the highest-value work left** (oteldb#1193). A and D are
   single-threaded and allocation-bound; every comparable engine fans out over series at
   `GOMAXPROCS`-ish degree, and the aggregation fold is commutative so the map-reduce is
   straightforward. It does not reduce memory — it converts a 4-second query into a sub-second one.
2. **Time-chunking (M16) bounds the step axis** (oteldb#1194), which is what E and D2 need. E's 1.2 GB is
   `series × steps`, and chunking is the only mitigation that covers the join build side, the
   pushdown pivot and group-heavy accumulators together.
3. **The schema's `O(matched series)` cost should be documented, then measured against
   alternatives.** 69 MB per 1M series is real but modest, and every design that removes it
   (interned symbols, ID-only refs with late label materialization) trades away the plan-time
   identity freezing that makes the engine's defect story good. Document first, optimize only if a
   profile demands it.
4. **Storage's eager `Fetch` undermines the engine's central claim** and should be treated as a
   prerequisite for M5's storage half, not a follow-up.
5. **Exposing stepped aggregates, then sliding-window aggregates, is the highest-value storage
   change** — it is what makes D-shaped queries cheap rather than merely bounded.
6. **Adopt VictoriaMetrics' pre-execution estimate and ClickHouse's tracker hierarchy for M11**,
   and skip `MaxSamples`; Mimir already abandoned it.
