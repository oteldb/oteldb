# Issue split — CROSS-ENGINE-REPORT.md → storage / oteldb

The profiling in `CROSS-ENGINE-REPORT.md` (§ pprof profiling) turned up a handful of
levers. This file splits them into fileable issues by repo and **reconciles each
against the existing tracker** — because most of the engine-level work is already
landed. Scan done 2026-07-01 against `oteldb/storage`, `oteldb/oteldb`,
`oteldb/promql-engine` (issues disabled there).

Profile build: oteldb `776c74eb` + storage `bee17bc`, `GOMEMLIMIT=1GiB`, 64 MiB
block cache, ~140k series. Headline: **query execution is ~80% GC** under the RSS
budget; the driver is allocation *rate* (~167 MB/s) against a large live heap.

---

## TL;DR — what's already done vs. what's live

| lever (from the report) | repo | existing issue | status |
|---|---|---|---|
| Stream k-way merge, stop materializing whole columns | storage | **#52 → #57 #58** | ✅ landed (`a06aa20`, `d0e9a3c`) |
| count() / column-need pruning (zero-decode count) | storage | **#53 → #37 #40** | ✅ landed (`7917aeb`) |
| Block-granular decode cache | storage | **#46 #38 #45** | ✅ landed (`b4da64f`) |
| Recycle/pool the *cached decodedPart* path | storage | **#39 #42 #60** | ✅ landed (`6f5f3ab`) |
| Label identity footprint at rest | storage | **#55 → #59** | ✅ partial (97→75 MiB) |
| **Pool block-decode-*miss* output buffers (`chunk.resize`)** | storage | **#54 (closed, "subsumed")** | ❗ **residual — see S1** |
| Sparse / pageable part index | storage | #48 | ⏳ open, lower priority |
| Head byte-column layout (record path) | storage | #49 | ⏳ open, scan-path |
| RSS-safe decode-cache default / total budget | oteldb | #1112 | ⏳ open (covers "don't grow cache") |
| PRW ingest double-conversion allocs/GC | oteldb | #1106 | ⏳ open (covers the mixed-profile ingest allocs) |
| **promql-engine eager full-series materialization** | oteldb | — | 🆕 **O1** |
| **Push range-vector aggregation down to engine** | oteldb | — | 🆕 **O2** |
| **Cross-query series→labels intern cache** | oteldb | — | 🆕 **O3** |

Net: the engine's structural fixes (stream merge, block cache, count pushdown,
decode budget) are **already in this build**. Only **one** storage issue is
genuinely live (S1, a residual #54 closed prematurely); the three new items are
oteldb-side query-layer work.

---

## STORAGE — github.com/oteldb/storage

### S1 — Pool block-decode *miss-path* buffers: `chunk.resize` is ~52% of query alloc-rate → ~80% GC under the 1 GiB budget

**Type:** enhancement · **Relates to:** #54 (reopen or supersede), #46, #58, #57

**Problem.** With the streaming merge (#58) and block cache (#46) landed, the *live*
heap is bounded (no more #52 growLen cliff). But under `GOMEMLIMIT=1GiB` the block
cache is pinned at its 64 MiB fraction floor, and a broad concurrent fetch
(8 jobs × `node_.+` / all-cpu selectors × many parts) misses that cache constantly.
Every miss re-decodes a block, and **each decode allocates fresh**:

- `block.decodeOneBlock` (`block/blockcolumn.go:336`) calls `dec(nil, stream)` and
  `comp.Decompress(nil, …)` — **nil dst**, so `chunk.resize`
  (`encoding/chunk/timestamp.go:260`) allocates a new `[]int64`/`[]float64` per
  block, plus a fresh decompress buffer.
- `blockCache` eviction (`engine/blockcache.go` `removeElem`/`removeElemKeepPrefix`)
  drops the entry from the list/index/byte-total but **does not recycle** the slice.

So evicted decoded blocks are pure garbage, re-minted on the next touch.

**Evidence** (query-only, ingest frozen; `pprof/cross-engine-q/`):

```
alloc_space:  chunk.resize[int64] 26.4% + chunk.resize[float64] 25.8%  = ~52%
              backend.cachedBackend.Read → PartReader.Column           = ~15%
cpu:          gcBgMarkWorker 79.8%  (scanObject 65.5%)                  ← the tax
```

**Why #54 doesn't already cover it.** #54 ("Pool decode column buffers
growLen/chunk.resize") was closed as *"subsumed by #46/#58; residual plan churn
pooled in #60."* #46/#58 removed the whole-column **live-heap** materialization; #60
pooled plan maps. Neither pools the **block-decode output on a cache miss** — which
is invisible in `inuse_space` (it's transient) but dominates `alloc_space`, and under
a hard `GOMEMLIMIT` the alloc *rate* is what pins GC at 80%.

**Proposal.**
1. A `sync.Pool` (or per-engine free-list) of `[]int64`/`[]float64` column buffers,
   sized to the block row-count. `decodeOneBlock` draws `dst` from it instead of
   passing `nil`; `blockCache` eviction returns the slice to the pool. Slices are
   pointer-free, so this is scan-safe and doesn't touch cache-entry immutability.
2. Same for the decompress scratch buffer (`comp.Decompress(dst, …)`).
3. Pool the raw column read buffers on `cachedBackend.Read` / `PartReader.Column`.

**Budget note.** Do **not** raise `decode_cache_bytes` to cut the miss rate — that
breaks the <1 GiB RSS target (oteldb/oteldb#1112). Pooling is budget-neutral: it cuts
alloc *rate* (→ GC frequency) without enlarging resident memory.

**Acceptance.** On the query-only profile, `chunk.resize` drops out of the top allocs
and `gcBgMarkWorker` CPU share falls from ~80% toward ~40%; `count_cpu_cores` p90
improves toward the ≤75 ms target with RSS held < 1 GiB.

### Already tracked — no new issue

- Stream merge / whole-column materialization → **#52, #57, #58** (closed, landed).
- count()/column-need pruning → **#53, #37, #40** (closed, landed).
- Label footprint at rest → **#55, #59** (interning; open follow-up value-byte work).
- Sparse/pageable part index → **#48** (open; lower priority per its own note).
- Head byte-column layout → **#49** (open; record/scan path, not metric query).
- RSS umbrella + profiles → **#51** (open; S1 is a concrete child of it).

---

## OTELDB — github.com/oteldb/oteldb (query layer; promql-engine issues are disabled)

The largest *live-heap* scan targets under query load are in the query engine, not
the storage engine. promql-engine is a fork (`oteldb/promql-engine`) with issues
off, so these are tracked here.

### O1 — Stream the promql-engine coalesce: stop buffering the whole matched series set

**Type:** enhancement/perf

`execution/exchange.concurrencyOperator.Series` (guarded by a `sync.Once`) and
`coalesce.loadSeries` buffer the **entire** matched series set before evaluation —
**317 MB live** for the `node_.+` / all-cpu selectors × 8 concurrent queries. It's the
single biggest thing GC re-scans every cycle (per-series objects + `labels.Labels`
are pointer-rich, unlike the pointer-free decoded columns). Evidence:
`inuse_space` — `coalesce.loadSeries.func1` 37% of live heap.

**Proposal.** Stream/chunk the coalesce so series are evaluated and released
incrementally, or bound the concurrency buffer, so the operator holds O(active) series
instead of O(all-matched). Cuts per-GC-cycle scan cost and the scan-query p99 tail
(tail = a GC pause landing on a big heap).

### O2 — Push range-vector aggregation down to the engine fetch

**Type:** enhancement/perf

`storage/prometheus.matrixSelector.loadSeries` pulls **every raw sample** in the
`[1m]`/`[5m]` window into memory (**213 MB live**) for `rate`/`irate`/`sum_over_time`,
then reduces in the engine. The storage engine already has an aggregate path
(`engine.aggViaStats`, `engine/seriesstats.go`).

**Proposal.** Wire the adapter (`internal/storagebackend`, `scanners.NewMatrixSelector`)
to request an engine aggregate/downsampled fetch when the outer op is a simple reducer,
so the engine returns per-step aggregates instead of raw samples. Cuts oteldb-side
materialization **and** the matching engine-side block decode (compounds with S1).

### O3 — Cross-query series→labels intern cache

**Type:** enhancement/perf

`storage/query/promql.querier.Select` (`query/promql/queryable.go:119`) has a
per-query `labelCache`, discarded when the query ends — so ~140k `labels.Labels`
(pointer-rich strings) are rebuilt and re-scanned by GC on **every** query.

**Proposal.** An engine-lifetime series→labels intern cache (invalidated on series
churn), so labels are built once and reused across queries. Cuts both the allocation
rate and the pointer-heavy live heap GC scans. (Code lives in the storage repo's
`query/promql`, but the caching policy is oteldb's call — file here, implement across
the boundary.)

### Already tracked — no new issue

- RSS-safe decode-cache default / total memory budget → **oteldb/oteldb#1112**
  (open) — covers the report's "64 MiB under the cap / don't grow the cache".
- PRW ingest double-conversion (PRW→pdata→metric.Metrics) dominating allocs/GC →
  **oteldb/oteldb#1106** (open) — covers the mixed-profile ingest allocation share.

---

## Filed (2026-07-01)

| draft | filed as | title |
|---|---|---|
| **S1** ✅ **DONE** | [oteldb/storage#62](https://github.com/oteldb/storage/issues/62) (PR #63, `de9aae3`) | Pool block-decode miss-path buffers — validated: `chunk.resize` eliminated, alloc rate −42%, GC-mark −25pp (see CROSS-ENGINE-REPORT.md § Re-benchmark). Bottleneck shifted to O1. |
| **O1** ✅ **DONE** (promql-engine v0.9.0, `45347ca`) | [oteldb/oteldb#1116](https://github.com/oteldb/oteldb/issues/1116) | `coalesce.releaseSeries()` releases the label set after resolution — coalesce buffer eliminated from live heap; at-rest RSS 963→856 MiB, live heap 863→627 MB. GC-CPU% flat (cuts live heap, not alloc rate). Next: query-path alloc *rate*. |
| **O2** ✅ merged (`ce98887c`), issue still open | [oteldb/oteldb#1117](https://github.com/oteldb/oteldb/issues/1117) | Range `*_over_time` pushdown — landed, but **not exercised** by the suite (its over_time queries are instant, its range queries are rate/irate). Extend to rate/irate + add a range over_time query. |
| **O3** ✅ **DONE** (`d06b7d4`, storage v0.24.0) | [oteldb/oteldb#1118](https://github.com/oteldb/oteldb/issues/1118) | Label intern cache — active (backend-lifetime), at-rest RSS 963→941 MiB; larger win masked until O1 lands |

S1 filed as a **new** storage issue linking the closed #54/#46/#58 (rather than
reopening #54). The "already tracked" rows above were left as-is on their existing
issues.

---

## Round 2 candidates (2026-07-02, from the clean idle-box comparison) — NOT yet filed

Diagnosis: GC-pacer thrash near GOMEMLIMIT (solo query = 44% GC; 8-way load pushes
live heap to 1297 MB > 1 GiB limit). Latency = alloc-per-query × resident-heap.
Details in CROSS-ENGINE-REPORT.md § "Clean head-to-head + optimization roadmap".

| id | repo | title | evidence |
|---|---|---|---|
| **S2** | storage | Recycle decode buffers at *unpin* / scale `blockBufCap` — freelist misses are 50.6% of query allocs | `bufFreeList.get` flat 1.92 GB/30s; cap=32 vs 8-way×prefetch-8 bursts; pins release only at teardown |
| **S3** | storage | Stop `slices.Clone` on backend-cache **hit** | `backend/cache.go:143`, 579 MB/30s (15.2%) |
| **S4** | storage | Pool `collectMany`/`ensureCap` merge output via `decPool` | 577 MB/30s (15.2%) |
| **S5** | storage | Grouped-count pushdown: `count by (label)` from postings/part-index (zero decode) | `count_cpu_cores` 212 vs 44 ms (4.8×); plain `count()` pushdown already beats ref (0.83×) |
| **S6** | storage | Frame resident-heap work (#48/#49/#51) as *latency*: pacer headroom | resident 700–850 MB of 1 GiB ⇒ GC every ~150–300 MB allocated |
| **O4** | oteldb | `rate`/`irate`/`increase` pushdown (extend #1117): per-(series,step) reset-aware fold | `cpu_wait` 6.7×, `cpu_usage` 2.5×; matrixSelector still ~39% of allocs |
| **O5** | oteldb | Fit decode/admission budget (#57) to GOMEMLIMIT so live heap never crosses the limit | 1297 MB inuse under 8-way vs 1024 MB limit → continuous GC |

### Round 2 — filed (2026-07-02)

| draft | filed as | note |
|---|---|---|
| S2+S3+S4 | [oteldb/storage#66](https://github.com/oteldb/storage/issues/66) | one issue, shared theme: transient query-buffer churn (~81% of query allocs) |
| S5 | [oteldb/storage#67](https://github.com/oteldb/storage/issues/67) | grouped-count / distinct-label-values pushdown (the 4.8× `count_cpu_cores`) |
| O5 | [oteldb/oteldb#1124](https://github.com/oteldb/oteldb/issues/1124) | fit admission budget to GOMEMLIMIT (heap crossed the limit: 1297 MB) |
| O4 | — already tracked as [oteldb/oteldb#1121](https://github.com/oteldb/oteldb/issues/1121) | commented with the 6.7×/2.5× prioritization evidence |
| S6 | — covered by storage#51/#48/#49 | commented on #51: resident heap = pacer headroom = latency |
| — | comment on [#1116](https://github.com/oteldb/oteldb/issues/1116) | O1 validation numbers posted so it can be closed |
