# Native PromQL engine

Design for a native, columnar, batched PromQL execution engine over the `oteldb/storage`
fetch seam, replacing the `github.com/oteldb/promql-engine` (Thanos) fork on the
`internal/storagebackend` path.

Status: implemented through M5 (selectors, range functions, aggregations including the
full-set forms and `count_values`, binary operators, instant functions, subqueries, concurrent
binop sides, storage pushdowns), plus M12 (the function tail), M13 (`absent`/`absent_over_time`)
and M16 (time-chunking), all taken out of order per their own notes, in `internal/scarecrow` —
6/21 upstream corpus files, 1016/2117 eval cases (§8.1; M16 doesn't move this number, it has no
corpus gate). Codename from the `gemini/scarecrow-engine-initial` prototype. Milestones and
their gates are in §10.

## 1. Scope

**In scope.** All PromQL evaluation for the `storagebackend` data path: parse (upstream
parser) → plan → execute over `oteldb/storage`'s `query/fetch` seam.

**Package.** `internal/scarecrow`.

**Out of scope.** `internal/chstorage` keeps the Thanos fork unchanged and frozen. chstorage is
on a deprecation path, so the fork retires with it and the end state is a single engine — the
two need to agree on semantics only for as long as chstorage is still served, and no permanent
dual-engine compliance burden is being taken on. The new engine never imports `ch-go`. This is
the single most important boundary in the design: the
prototype's `Block` was built out of `proto.ColUInt32`/`ColInt64`/`ColFloat64` — a ClickHouse
wire type used as an in-memory execution type. Dropping that is what makes a clean data model
possible.

**Frontend.** `github.com/prometheus/prometheus/promql/parser`. The engine consumes
`parser.Expr` and implements `promql.QueryEngine`, so it is a drop-in for the existing
`internal/promql.Engine` interface (identical method set) and — critically — can be driven by
`promqltest.RunBuiltinTests` (§8).

## 2. Why not the Thanos fork

Not correctness — the fork is correct and battle-tested. Four structural reasons:

1. **Row-shaped seam.** The fork is reached through `storage.Queryable`/`SeriesSet`/
   `chunkenc.Iterator`. `oteldb/storage` produces `fetch.Batch{Timestamps []int64, Values
   []float64}` — already columnar, already pooled. Today `query/promql.Queryable` re-wraps
   those slices into a per-sample `chunkenc.Iterator`, and the fork immediately unwraps them
   back into `[]float64`. Two shape changes and a per-sample interface call for nothing.
2. **Pushdowns are bolted on.** `internal/storagebackend` already carries three pushdowns
   (`aggregateOverTimeOp`, `Counter`, `GroupCounter`) implemented by *intercepting scanner
   construction* (`scanners.NewMatrixSelector`) and pattern-matching the logical node. That is
   the wrong seam: there is no plan to rewrite, so each new pushdown is another special case
   in an adapter. A native planner makes pushdown a rewrite rule.
3. **Two tracked memory problems are structural to the fork's shape.**
   [oteldb#1116](https://github.com/oteldb/oteldb/issues/1116) — `coalesce.loadSeries` buffers
   the entire matched series set (~317 MB live, 37% of heap) — and
   [oteldb#1117](https://github.com/oteldb/oteldb/issues/1117) — `matrixSelector.loadSeries`
   materializes every raw sample in the window (~213 MB live) — are both consequences of
   step-major evaluation over a row-shaped seam. Series-major removes the class rather than
   tuning it: nothing is proportional to the matched series set (§4.6), and exactly one series'
   raw samples are live at a time (§4.3). They close when the engine replaces the fork on this
   path, which is M5's gate.
4. **`fetch.Batch` fields we cannot express.** `ScaleFactors` (lossy-sampling weights) is
   produced by the storage engine and consumed by nobody on the read path — see
   `docs/storage-integration.md:97`. Sampled metrics currently yield biased `sum`/`rate`/
   `count`. The fold from raw samples to the step grid is the correct place to apply the
   weight, and in the fork we do not own that fold.

## 3. Data model

Two layouts, one materialization point between them.

### 3.1 Raw level — borrowed, ragged, zero-copy

Storage hands us one series per `fetch.Batch`, with irregular timestamps. The engine borrows
those slices verbatim:

```go
// Samples is one series' raw samples, borrowed from the producing fetcher.
// The slices alias fetch.Batch and are invalid after Release.
type Samples struct {
    Ref     SeriesRef
    T       []int64   // unix millis, ascending
    V       []float64
    Weights []float64 // ScaleFactors; nil ⇒ every weight is 1
}
```

This is the "zero-copy" in the goal list, stated precisely: **no copy between storage and the
selector operator**. Nothing above the selector ever sees `Samples`.

Lifetime rule, and it is not negotiable: a selector consumes a batch, folds it, and calls
`Batch.Release()`. No operator retains a `Samples`. The prototype violates this everywhere —
`StepEvaluator`, `RangeFunctionOperator` and `SharedOperator` each drain their entire input
into a `map[uint32]*seriesSamples` before producing anything, which defeats batching, defeats
pooling, and makes memory proportional to the query's whole result set.

### 3.2 Step level — one series column at a time

Above the selector, PromQL is defined on a fixed step grid: every operator produces exactly
one value per `(series, step)`. The engine's currency is one series' slice of that grid:

```go
// Column is one series' values across every step of the current chunk.
// The step timestamps are shared per chunk and live on the eval context, not here.
type Column struct {
    Ref   SeriesRef
    V     []float64 // len = stepsInChunk
    Valid []uint64  // bitset over V; 0 ⇒ no sample at that step
}
```

**Why series-major, not step-major.** An earlier draft of this document used a dense
`(step × series)` tile, copying Thanos' `StepVector`. That was wrong, and the reasoning is
worth recording because it is the crux of the whole design.

Thanos iterates steps outermost and series innermost because Prometheus' TSDB hands it a
**seekable cursor per series** (`chunkenc.Iterator`); holding thousands of cursors open and
advancing them together is cheap, so a step-major tile is the natural shape. `fetch.Iterator`
is not a cursor — it is a one-shot forward stream of *complete series*. Copying a layout whose
justification is a storage property we do not have produced an impedance mismatch, and every
consequence that followed was a symptom of it:

| Symptom of step-major | Under series-major |
|---|---|
| selector must drain all series before emitting tile 0 | gone — a column is ready when its batch arrives |
| `O(series × steps)` resident at the selector | `O(groups × steps)`, set by the consumer |
| `Transpose` operator for subqueries under range functions | gone — a column *is* the step-axis traversal |
| global tile boundaries as a tree-wide invariant | gone — no tiling |
| two execution modes | one |

Series-major matches storage's delivery order exactly, so there is no transpose anywhere in
the engine. VictoriaMetrics — the fastest Go PromQL implementation, and a stated reference for
this work — is built this way: `evalRollupFunc` computes per-series over a shared timestamp
grid, then aggregations combine the resulting series. Thanos is the outlier, for cursor
reasons that do not apply to us.

**It is also the better SIMD shape.** Under step-major, `sum by` is a strided reduction across
the series axis. Under series-major it is `AddF64(accRow, accRow, col)` — a stride-1
elementwise vector add of a column into an accumulator row, with the reduction happening
implicitly as many columns accumulate into the same row. Unary functions are stride-1 too.
Every kernel becomes elementwise over contiguous `[]float64` (§7).

**Why a bitset instead of NaN.** PromQL distinguishes "absent" from "NaN value", so absence
needs its own channel regardless. Making it a bitset (rather than a `[]bool` or a branch per
sample) is what lets kernels be branch-free: compute every lane, mask afterwards. Branchy
per-sample `if ok` is the single biggest obstacle to vectorization.

**Chunk sizing.** A column costs `stepsInChunk × 8B` — 1.9 KB at 240 steps, L1-resident. The
binding constraint is not the column but the largest accumulator in the tree
(`numGroups × stepsInChunk × 8B`), so the planner picks `stepsInChunk` to keep that under a
budget and splits long queries into sequential chunks (§4.4). Unlike the tile boundaries this
replaces, chunking is a memory policy, not a correctness invariant — nothing in the operator
contract depends on where the cuts fall.

### 3.3 Schema — resolved before execution

```go
type Schema struct {
    Series []labels.Labels // SeriesRef is an index into this slice
    Hashes []uint64        // memoized labels.Hash, for matching/grouping
    Scalar bool            // scalar-typed: exactly one anonymous "series"
}
```

Series identity is frozen for the whole tree in **one bottom-up pass at plan time**, before
any `Next` call. Columns then carry only an index into their producing operator's schema.

This is a deliberate response to a defect class the prototype fought repeatedly (commits
`b57600f8 fix(scarecrow): ensure Series ID consistency across operator tree`, `2cbf863f
fix(scarecrow): improve engine correctness and determinism`, `d7ec6e6f fix(promql): sort
matchers in vector selector for consistent output`): there, IDs were minted lazily during
execution by `getOrCreateSeries(lb, cache map[uint64]uint32)`, so an operator's ID space
depended on the order data happened to arrive. Resolving schemas eagerly makes that
unrepresentable.

Cost: the selector must enumerate its matching series before producing data. `oteldb/storage`
supports this — series enumeration is an index operation, separate from sample decode.

### 3.4 On-demand materialization

Not every query needs values, and `count` should never touch them. This splits into two
independent axes, and `oteldb/storage` is in very different shape on each.

**Axis 1 — column need.** The storage engine already models this internally:
`engine.colNeed{values bool}` selects a timestamps-only decode, `decodedPart.haveValues`
keeps the decode cache from serving a ts-only entry to a value-needing query, and
`plan.acquireDecodeBudget` reserves memory accordingly (`engine/part.go:373`,
`engine/engine.go:393`). `Engine.Count` goes further than skipping values: it answers from
the part index plus a timestamps-only *edge* decode, so a `count()` over a matched selector
usually decodes nothing at all (`engine/count.go:22`).

The problem is reach. That machinery is **not exposed through the `fetch.Request` seam**.
`Request.Projection` is logs-only — "the columns to materialize for surviving rows". The
metrics `Fetch` path hardcodes `colNeed{values: true}`, so every non-pushdown query pays a
full `ts+values` decode and copy. The only doors to the cheap paths are the pre-baked
capability interfaces: `Counter`, `GroupCounter`, `AggregateMetrics(Named)`. Each is one
fixed query shape; there is no way to say "these series, timestamps only".

So today the answer is: **`count` is fully lazy (via `Counter`), and nothing else is.**
Queries that need strictly less than values but have no matching capability still decode
everything:

| Query | Needs | Gets today |
|---|---|---|
| `count(sel)` | series existence | existence — `Counter`, no decode |
| `count_over_time(sel[5m])` | per-series count | count — aggregate sidecar |
| `timestamp(sel)` | timestamps | ts + values |
| `present_over_time`, `absent_over_time` | existence per step | ts + values |
| `last_over_time(sel[5m])`, plain instant selector | last sample per step | the whole window |

The engine's part is to *compute* the need and carry it: the planner derives a `Need` lattice
(`NeedExistence < NeedTimestamps < NeedValues`) top-down from each consuming operator to its
selector, and the selector puts it on the request. That is cheap and useful immediately for
pushdown-rule matching (§5.2). Turning it into actual decode savings needs a storage-side
change — see below.

**Axis 2 — window granularity.** Storage decodes at sub-part granularity already
(`MetricBlockRows` makes part columns independently decodable blocks, with `blockCache` and
`DecodeMemoryBytes` admission control). But `Fetch` still assembles **one `fetch.Batch` per
series covering the entire query window** — `m.collect(tsBuf, valBuf)` produces the complete
`ts`/`values` for that series before the batch is handed over (`engine/engine.go:440`).

This bounds the raw level at one whole series' window: a 30-day range at 15s is ~172k samples
≈ 2.7 MB of `ts+values` for a *single* series. Since the engine holds exactly one series'
`Samples` at a time (§4.3), peak raw memory is `shardConcurrency × series-window` — an amount
the engine controls through shard degree, but not one it can shrink below a single series.
That floor is the cost of a non-chunked seam, and it is why ask #2 below stays on the list even
though the execution model no longer depends on it.

**Engine-side obligations, available now:**

- Set `Request.Recycle` and call `Batch.Release()` after the fold. Pooling is opt-in
  (`query/fetch/fetch.go:105`); not setting it silently forfeits buffer reuse for every query.
- Derive and carry `Need`, even before storage can act on it, so pushdown rules match on a
  computed property rather than re-deriving preconditions per rule.
- Time-chunk long range queries at the planner level (split the query window, evaluate
  sequentially, concatenate), sized from the widest accumulator in the tree (§4.4).

**Storage-side asks, in rough value order.** These are `oteldb/storage` issues, not engine
work, and the engine is useful without them:

1. Expose column need on `fetch.Request` for metrics (a `Values bool` / `Need` field routed
   to the existing `colNeed`). The mechanism exists; only the seam is missing. Unlocks
   `timestamp()`, `present_over_time`, `absent_over_time` and every existence-shaped query.
2. Chunked metric batches — let a fetcher emit a series' window as several batches rather
   than one, bounding the raw level.
3. A step-aligned bucket aggregate covering `rate`/`increase` (the sidecar today answers only
   `sum`/`count`/`min`/`max`), which would push the single most common PromQL shape down
   entirely. Already tracked as [oteldb#1121](https://github.com/oteldb/oteldb/issues/1121):
   `engine.SeriesAgg` would need per-window first/last value+timestamp and counter-reset
   accounting, which is a migration-shaped storage change.

### 3.5 Sampling weights

`fetch.Batch.ScaleFactors` is a load-shedding artifact, not a metrics feature. When a tenant
exceeds `MaxRowsPerSecond`, storage does not reject the overflow (that would leave gaps) — it
keeps 1 point in N and tags each survivor with weight N, so counts and sums can be scaled back
up at read time (`admission.go:117`). The sampler is adaptive (`winSF = ceil(observed/budget)`,
recomputed each 1s window) and **deterministic** — `sampleHash(seriesID, ts) % N == 0` decides
keep/drop, so the same point survives consistently across batches and cluster nodes. Under
budget it returns nil weights and the path allocates nothing, which is why `ScaleFactors` is
nil in the common case.

Nothing consumes it on the read path today, so a sampled tenant would read low. It is dormant
in practice (oteldb never sets `MaxRowsPerSecond`, so every weight is 1), but it is expected to
stay, and the fold is the only place it can be applied — so the engine handles it from M1
rather than retrofitting every range function later.

**Weights are consumed entirely inside the leaf fold.** `Column` has no weight channel and no
operator above a selector ever sees one. This follows from §4.3: weights live on `Samples`,
and `Samples` never crosses an operator boundary.

**Policy matrix.** The one-line summary in the storage comment ("multiplies it back into
count/sum/rate") is too coarse — whether `rate` should be weighted depends on temporality,
which `signal/metric` carries per series as `LabelTemporality`:

| Function | Weighted | Rationale |
|---|---|---|
| `count_over_time` | `Σsf` | each survivor stands for `sf` samples |
| `sum_over_time` | `Σ(v·sf)` | same, value-scaled |
| `avg_over_time` | `Σ(v·sf) / Σsf` | frequency-weighted mean |
| `stddev`/`stdvar_over_time` | frequency weights | `Σ(v²·sf)/Σsf − mean²` |
| `rate`/`increase`/`delta`, **cumulative** | **no** | cumulative values survive subsampling intact; `(last−first)/dt` is already unbiased, weighting inflates by `N` |
| `rate`/`increase`, **delta temporality** | — | undefined; see [#1190](https://github.com/oteldb/oteldb/issues/1190) |
| `irate`/`idelta` | no | defined on the last two samples; weight is meaningless |
| `min`/`max_over_time` | no | extremes of the kept subset — biased inward, not fixable by weighting |
| `quantile_over_time` | no | weighted exact quantiles are ill-defined in PromQL semantics |
| instant selector, `last_over_time` | no | a single value, not an aggregate |
| `present`/`absent_over_time` | no | existence |
| `changes`, `resets` | no | undercount, not fixable by weighting |

Only the cumulative rule is specified, and it is the one implemented. The delta row is blank on
purpose: oteldb does no delta→cumulative conversion, so `rate()` over a delta-temporality series
is already meaningless before sampling enters the picture ([#1190](https://github.com/oteldb/oteldb/issues/1190)).
Writing a weighting rule for it would imply a semantics that does not exist.

Aggregations across series (`sum by`, …) need no policy — they consume step values whose weights
were already folded in.

**Two residual biases, documented and not fixed.** On a subsampled *cumulative* counter, a
reset occurring between two dropped samples is invisible, so `resets` undercounts and `rate`'s
reset correction can miss one, biasing low. And `max_over_time` sees only survivors. Neither is
recoverable from a weight; both are inherent to lossy sampling.

**Testable, because sampling is deterministic.** A golden test ingests a corpus twice — once
unsampled, once with `MaxRowsPerSecond` forcing `sf = 4` — and asserts the weighted result
matches within tolerance. This is only possible because `sampleHash` is reproducible; it makes
the matrix above verifiable rather than aspirational.

## 4. Execution model

### 4.1 Interface

```go
type Operator interface {
    fmt.Stringer

    // Schema returns this operator's output series set. Called once, at plan time,
    // bottom-up. Must be deterministic and independent of execution.
    Schema(ctx context.Context) (*Schema, error)

    // Next returns this operator's next output series column for the current chunk,
    // or nil at end of stream. The returned Column is owned by the operator and valid
    // only until the next Next or Close call. Callers MUST NOT retain it.
    Next(ctx context.Context) (*Column, error)

    Children() []Operator
    Close() error
}
```

One interface, one mode. `Next` returns an operator-owned borrow rather than filling a caller
buffer, so every operator owns exactly one output column and reuses it for the whole chunk —
pooling with no pool.

The borrow rule means an operator with two consumers is illegal. That is intentional: sharing
is a *planning* concern (§5.3), not something to paper over at execution time with the
prototype's `SharedOperator`, which buffers its child's entire output into `[]Block` under a
`sync.Once` — unbounded memory, and it silently serializes the very branches concurrency was
supposed to overlap.

### 4.2 The driving loop

**`Next` is called once per output series.** Not per storage batch, and not per step. An
operator emitting `n` series is called `n+1` times per chunk (the last returning nil). Because
each operator has its own series set, call counts differ down the tree: a `sum by (job)` over
1000 series is called 11 times while its child is called 1001 times.

```
for each time chunk:                    // planner-chosen, bounds accumulators
    for each series shard:              // §6, independent pipelines
        for {
            c, err := root.Next(ctx)    // one output series' full column
            if c == nil { break }
            encode(c)
        }
```

Operators fall into two shapes, and the distinction is the whole execution model:

- **Streaming** — one `Next` in, one `Next` out. Unary functions, scalar binops, label
  manipulation. Resident: one column.
- **Accumulating** — drains its child completely on the first `Next`, building an accumulator,
  then emits from it. Aggregations, binops, `topk`, `quantile`. Resident: the accumulator.

There is no third shape, and no operator ever sees more than one series at a time on its
input. What varies is only how much an operator chooses to remember.

### 4.3 `Samples` never crosses an operator boundary

The raw ragged level (§3.1) is *internal to selector operators*. `Column` is the only thing
that crosses `Next`. Two operators consume `Samples`, both leaves:

- **`VectorSelect`** — raw samples → step grid by lookback/staleness → `Column`.
- **`MatrixFold`** — raw samples → apply `rate`/`increase`/`*_over_time` over the window
  `(t-range, t]` at each step → `Column`. The matrix selector and its function call are **one
  fused operator**, never two; there is no "matrix" type and no operator emits a range vector.

Both consume exactly one `fetch.Batch`, fold it, `Release()` it, and return the column. One
series' raw samples are live at a time, regardless of query cardinality — the raw level is
`O(1)` in series, always.

This is why "one materialization point" (§3.1) is a claim about the whole engine and not just
about storage: the ragged→dense fold happens once, in a leaf, and everything above is
contiguous `[]float64`.

### 4.4 What each operator costs

Resident memory is set entirely by the accumulating operators. Since every consumer is fed one
column at a time, "what does this operator have to remember" is answerable per operator:

| Operator | Accumulator | Cost |
|---|---|---|
| unary fn, scalar binop, label ops | none | `O(steps)` |
| `sum`/`count`/`min`/`max`/`avg`/`group`/`stddev`/`stdvar` | one row per group | `O(groups × steps)` |
| `topk`/`bottomk(k)` | per-step bounded heap | `O(k × steps)` |
| vector-vector binop | hash table on the build side | `O(probedBuildSeries × steps)` |
| set operator (`and`/`or`/`unless`) | output rows plus one side's step coverage | `O(outputSeries × steps)` |
| `quantile`, `count_values`, `sort` | every input value | `O(series × steps)` |
| bare selector (no consumer above) | none — streams to the encoder | `O(steps)` |

Every `steps` in this table means one chunk's steps, not the whole query's, as of M16 below:
chunking re-runs the entire operator tree per chunk against a chunk-scoped `EvalContext`, so
every row here — including the three data-dependent ones — is bounded by `Opts.ChunkSteps`
regardless of how long the query's actual range is.

Three rows deserve comment. The vector binop buffers only its **build side** — the "one" side of
a match — and only those of its series some many-side series actually pairs with, which is known
at plan time because matching is resolved from label sets alone. `group_left`/`group_right`
therefore buffer *less* than a plain one-to-one binop, where every series is its own match group.
Set operators buffer no input at all: `and`/`unless` need the rhs's step coverage but never its
values, and `or` emits the lhs unconditionally, so ordering the two drains per operator removes
the buffer entirely. `quantile` is `O(series × steps)` because exact quantiles need the
full per-step set; Prometheus and Thanos pay the same, and it is inherent, not a layout
artifact. The **bare selector** row is the one the step-major draft got badly wrong: a query
like `http_requests_total[1h:15s]` with no aggregation was `O(series × steps)` resident there,
but here each series' column streams straight to the response encoder and is released. The
response body is large; the engine's footprint is not.

**Chunking.** *Done (M16).* Long ranges are split into sequential chunks so that the largest
accumulator stays under budget — `sum by (instance)` where groups ≈ series is genuinely
`O(series × steps)`, and 30 days at 15s is 172k steps. `query.execRange` splits `stepGrid`'s
output into `Opts.ChunkSteps`-sized windows (default 10,000) and re-plans and re-runs the whole
operator tree per chunk against a chunk-scoped `EvalContext`, concatenating each chunk's
`collectRange` result by series identity (`rangeMerger`, keyed by label hash — stable across
chunks because no operator's *schema* depends on values, only full-set operators' data-dependent
*output selection* does, and that selection is itself already per-step, so a chunk boundary
changes nothing about which series survive at a given step). Parts are time-partitioned, so a
chunk's fetch touches only its own parts and decodes nothing extra.

This is a fixed, configured budget rather than the dynamic "size `stepsInChunk` from the widest
accumulator in the tree" this section originally proposed: resolving that width would mean
running a data-dependent operator's `Schema()` (§4.4's `quantile`/`topk`/`aggregateOverTime`
row) once to measure it and again per chunk, defeating the exact operators chunking exists to
bound. A static default is simpler and already delivers the property that matters — peak
resident is flat in range length, not in cardinality — leaving per-query tuning to `Opts` rather
than a planner heuristic.

No corpus gate (the corpus is short-range); verified instead by a differential suite
(`chunk_test.go`) proving chunked and unchunked evaluation agree with each other and with the
upstream engine down to `ChunkSteps: 1`, across selectors, aggregations, one-to-one binops,
subqueries and every full-set aggregation, and by a memory-archetype benchmark (`F_time_chunking`
in `memprofile_test.go`) showing a lower peak for a one-to-one join chunked against the same join
run unchunked.

**Storage ask, unchanged in value but not in urgency.** Time-chunk-major `Fetch` iteration
(§3.4, ask #2) would let chunking collapse into the iterator and drop the repeated postings
resolution. Worth doing; no longer close to critical.

### 4.5 Worked example

`sum by (job) (rate(http_requests_total[5m]))`, range query over 1h at 15s step, 1000 series,
10 jobs.

**Plan time.** Schemas resolve bottom-up: `MatrixFold(rate, 5m)` enumerates 1000 series from
the postings index; `Aggregate(sum by job)` derives its 10 output groups from those 1000 label
sets. All `SeriesRef`s are frozen. 240 steps; the widest accumulator is `10 × 240` = 19 KB, so
one chunk suffices.

**Execution.** `Aggregate.Next()` #1 allocates the `10 × 240` accumulator and drains its child.
Each `MatrixFold.Next()` takes one arriving `fetch.Batch`, computes `rate` over the 5m window
at each of the 240 steps into its 1.9 KB output column, releases the batch, and returns the
column; `Aggregate` runs `AddF64` of that column into the row for the series' `job`. After
1000 such calls the child returns nil and `Aggregate` emits group 0's column. Calls #2–#10
emit the rest; #11 returns nil.

Peak resident: **19 KB accumulator + two 1.9 KB columns + one series' raw samples.** Storage
I/O is a single streaming pass. No tile, no transpose, no chunk matrix.

Note what did *not* happen: no per-sample map lookup, no interface call per sample, no series
ID minted during execution, no operator saw a range vector, and no operator held more than one
input series.

### 4.6 Comparison

1000 series, `sum by (job)(rate(x[5m]))`, 10 jobs:

| | Prometheus/Thanos | Step-major draft | This design |
|---|---|---|---|
| model | step-outer, cursor per series | tile, two modes | series-outer, one mode |
| 1h @ 15s (240 steps) | ~2–4 MB | 1.9 MB | **19 KB** |
| 30d @ 15s (172k steps) | ~2–4 MB | 1.4 GB (needs chunking) | **14 MB** |
| scaling | `O(series)` | `O(series × steps)` | `O(groups × steps)` |

The `O(series)` term is gone from the read path: a selector's input series are consumed one at a
time and released, so nothing between storage and the first accumulator scales with matched
cardinality. That is a better asymptotic than the cursor model, not merely a recovery from the
step-major regression.

**Two exceptions, and neither should be buried.**

The first is the **schema**. Plan-time resolution (§3.3) holds every matched series' label set for
the query's lifetime — measured at **69 MB per 1,000,000 series** (~72 B/series including the
memoized hashes). That is `O(matched series)`, the same class as the fork's `coalesce.loadSeries`
(#1116), at roughly a third of its constant because less per-series state is retained. So the
engine eliminates #1117's raw-sample buffering outright and *reduces* #1116 rather than removing
it. Freezing identity before execution is what makes ref drift unrepresentable, so this is a
deliberate trade, not an oversight — but it must be stated as one.

The second is that a *one-to-one* vector binop has no small side —
in `a / on(instance) b` every series is its own match group — so its build side is a full matched
series set, `O(series × steps)` (§4.4). `group_left`/`group_right` do not have this problem: the
"one" side is a metadata metric with at most one series per key, so the buffer is proportional to
output groups.

So the accurate claim is narrower than "nothing is proportional to input cardinality": the engine
removes that buffering from the *sample* path, and the schema and a one-to-one join each keep a
version of it at a much smaller constant — one folded value per step rather than every raw
sample in every window, roughly 20× less at a 5m window and 15s step, and on one side rather than
both. Bounding it is what makes M16's chunking load-bearing rather than a knob. Both exceptions, and
the four workload archetypes they were measured against, are in `docs/promql-workloads.md`.

### 4.7 Concurrency and the borrow rule

`Concurrent` (§6) runs its child ahead in a goroutine, so it cannot hand out the child's
borrowed column — the child would overwrite it. It copies each child column into one of a
small ring it owns: a ~2 KB `memcpy` per series at the few points the planner inserts it.
Every other operator keeps the zero-copy borrow.

### 4.8 What this costs

Honesty about the trade-offs of series-major:

- **Instant queries lose vector width.** At one step a column is a single float, so `sum` over
  100k series is 100k scalar adds where a step-major tile would give one 100k-wide reduction.
  The reduction is a negligible share of that query's work (the per-series `rate` fold over raw
  samples dominates, and is series-at-a-time in either model). If it ever shows up in a
  profile, the fix is to batch series into one kernel call on the small-N path — see §7.1,
  which scopes exactly when that pays. It is an optimization inside one operator, not a second
  execution mode.
- **High group cardinality touches the accumulator randomly.** `by (instance)` with 10k groups
  × 2048 steps is a 160 MB accumulator, and each incoming column updates one 16 KB row at a
  random offset. Chunking is the lever; the tiled model had the identical problem plus a
  larger constant.
- **Latency to first result is unchanged.** An aggregation still cannot emit until its input
  is drained. Series-major makes that cheap, not incremental.

## 5. Planner

`parser.Expr` → logical plan → physical operator tree. Passes, in order:

### 5.1 Normalization and window analysis

Fold constants, canonicalize `on`/`ignoring` and matcher order, resolve `@` and `offset` into
an absolute evaluation window per selector, and compute each selector's required fetch window
(`[start - range - lookback - offset, end - offset]`). Instant queries are the `step == 0`
degenerate case of the range grid, not a separate code path — the prototype branched on
`o.Step == 0` inside every operator's `Next`, duplicating the window arithmetic five times.

### 5.2 Pushdown rules

The seam that makes pushdowns first-class instead of adapter special-cases. Each rule matches
a plan shape and replaces a subtree with a storage-answered operator, and each must state its
own preconditions:

| Rule | Replaces | Precondition |
|---|---|---|
| `count(sel)` → `fetch.Counter` | selector + count agg | fetcher implements `Counter` |
| `count by (l)(sel)` → `fetch.GroupCounter` | selector + grouped count | fetcher implements `GroupCounter` |
| `{sum,count,min,max,avg,present}_over_time` → `AggregateMetricsNamed` | matrix selector + fold | no projection, no per-series filter, no `@` |
| matcher pushdown | selector | index-safe matchers only (see `query/promql` doc comment) |
| `Need` narrowing | selector | derived per §3.4; effective only once storage exposes column need |
| join narrowing ([#1191](https://github.com/oteldb/oteldb/issues/1191)) | both selectors under a vector binop | `on(...)` matching (see below) |

The count, grouped-count and over-time rules exist today in `internal/storagebackend` (`overtime.go`, `overtime_range.go`,
`storagebackend.go`) and **must be ported before the engine can replace the fork**, or those
queries regress hard. Porting them into rules is a large simplification: `scanners.
NewMatrixSelector` currently reimplements the precondition check inline.

Room this opens up that the adapter seam could not: pushing `topk`/`bottomk` limits,
`__name__` regex narrowing, and — once the storage side grows it — a step-aligned bucket
aggregate covering `rate` directly.

#### Vector-binop joins

A vector binop is a hash join whose build side must be resident before the probe side can be
combined (§4.4). The join *itself* costs nothing to plan: matching is resolved entirely from
label sets at plan time, because `Scanner.Series` is an index-only call that materializes no
timestamps or values. What is resident is only the build side's **values**.

Three ways to shrink or remove that, in descending value-per-effort.

**1. Join narrowing ([#1191](https://github.com/oteldb/oteldb/issues/1191)) — implementable
today, and symmetric.** Both sides' series are enumerated
at plan time, so the exact intersection of matching signatures is known before a single value is
fetched. Both scans can then be restricted to it: an unmatched build-side series is never
probed, and an unmatched probe-side series is dropped by `pairOf == -1` anyway. For
`a * on(job) b` where `a` spans 10k jobs and `b` covers 50, both sides fetch 50.

The engine currently does the weaker half of this — it declines to *retain* an unprobed
build-side series (`vectorBinop.probed`), but storage has already fetched it. Narrowing turns
"fetch and discard" into "never fetch".

Precondition: **`on(...)` matching only.** The signature is then a fixed label set, so
"signature ∈ set" becomes an index-safe matcher (`job=~"a|b|c"`). Under `ignoring(...)` the
signature is "every label except these", which no single matcher expresses — that case needs a
series-ID set on `fetch.Request`, which is the one real argument for adding one.

**2. Caller-specified scan order — removes the build table entirely.** If both sides were
delivered sorted by matching signature, the join becomes a sort-merge: co-iterate, nothing
buffered, `O(1)` series resident per side. Note this is *not* what sorted delivery gives today —
`queryableScanner` asks for label-sorted output, but a signature is a label *subset*, so
full-label order is not signature order.

On a single node the storage change is small: `Engine.Fetch` resolves ids from the index and
`planFetch` fills each series' identity *before* any decode, and emission order is just that id
slice — so a caller key means sorting `(ids, series)` after planning, `O(n log n)` on series
count with no extra I/O, plus a field on `fetch.Request`.

The fan-out used to be the obstacle, and not for the reason the phrase "merges by
`signal.SeriesID`" suggests: `query/fetch.Merge` was **not** an ordered merge. It `Drain`ed every
child, hash-grouped into a `map[signal.SeriesID]*mergeAcc` ordered by *first appearance*, and
deep-copied — so the engine's `O(1)`-in-series raw level (§4.3) held for a single-node fetcher but
**not behind a fan-out**, and there was no k-way merge to parameterize with a comparator. Filed as
[oteldb/storage#208](https://github.com/oteldb/storage/issues/208) and
[#211](https://github.com/oteldb/storage/issues/211) (the base producer was materializing too, so
fixing `Merge` alone would not have made a single-child fetch stream).

**Both are fixed in storage v0.34.0.** `Fetch` gathers one series per `Next`, and `Merge` is now a
heap-ordered k-way merge holding one pending batch per child, so peak is `O(children)`. That
restores §4.3's property behind a fan-out, and it leaves exactly the ordered-merge machinery a
caller-specified key would need.

What it does **not** give is join order. The join matches on **signature**, a label subset; two
series that must join carry different labels (`__name__` differs at minimum) and therefore
different content-addressed ids, so id order and signature order are unrelated. A caller key is
still a `fetch.Request` field plus a sort of `(ids, series)` after planning — now cheaper to add
than to design, but still unbuilt.

**3. Point lookup by series — a memory tool, not a pipelining one.** Fetching the one-side
series on demand sounds like it removes the build table, but `group_left` sends many probes at
the same build series, so it needs a cache — and a cache of fetched series *is* a build table,
populated lazily. It also trades one sequential scan for N random lookups. Its real value is
therefore a bounded, LRU-backed build table that caps memory rather than holding every
build-side series, which belongs with M11's resource limits.

### 5.3 Common subexpression elimination

Identical subtrees (same expr, same window) are unified into one node. Where a unified node
has multiple consumers, insert an explicit `Tee` with a **bounded** ring buffer per consumer;
a consumer that falls behind the buffer applies backpressure to the producer. Bounded is the
point: the prototype's equivalent was unbounded.

### 5.4 Physical selection

Choose kernels, insert `Concurrent` wrappers (§6), pick series-sharding degree, and size
`stepsInChunk` from the widest accumulator in the tree (§4.4).

Subqueries need no special handling: a subquery's inner result is a `Column` on the inner
grid, and an outer range function reading along the step axis is reading that column
contiguously. The `Transpose` operator an earlier draft required here does not exist.

## 6. Parallelism

Three axes, and one structural rule.

**The rule: concurrency lives in exactly one operator.** `Concurrent` runs its child in a
goroutine feeding a bounded channel of columns; every other operator is single-threaded and
synchronous. Nothing else spawns goroutines, nothing else blocks on anything it does not own.
This is what makes the engine deadlock-free by construction. The prototype instead sprinkled
`errgroup` across `BinaryOperator` and `FunctionOperator` and spent four commits on the
consequences (`86048053 perf(scarecrow): fix deadlock in binary operator`, `96bbf094
fix(scarecrow): fix deadlocks and multi-block streaming in operators`).

1. **Series sharding.** A selector's series set is partitioned into N shards, each an
   independent pipeline. Under series-major this is a clean map-reduce: each shard accumulates
   into its own private accumulator and the shards' accumulators merge elementwise at the end
   (`sum`/`count`/`min`/`max` always; `avg` as sum+count; `topk`/`quantile` need the full set
   and act as shard barriers). The merge is itself an `AddF64`-shaped kernel over
   `groups × steps`. This is the axis that actually scales.
2. **Independent subtrees.** Binop LHS/RHS, function arguments, and each subquery get a
   `Concurrent` wrapper — overlapping their storage latency, which is the dominant cost.
3. **Storage fan-out.** Already inside `oteldb/storage` (parts, cluster). The engine must not
   duplicate it.

A single process-wide semaphore sized to `GOMAXPROCS` bounds concurrent `Concurrent`
operators across all in-flight queries, so a 50-subtree query does not spawn 50 scanners.

## 7. Kernels and SIMD

Kernels live in `internal/promqlengine/kernel` and have one shape:

```go
func AddF64(dst, a, b []float64)
func SumMasked(vals []float64, valid []uint64) (sum float64, n int)
```

Contiguous `[]float64` in and out, no branches in the loop body, no interface calls, validity
handled by mask. Series-major (§3.2) is what makes this the *only* kernel shape needed: a
column is contiguous, so unary functions, binops, and accumulation into a group row are all
elementwise over `stepsInChunk` elements. There is no strided or gathering kernel in the
engine. Every kernel has a pure-Go reference implementation; a build-tagged
`kernel_amd64.s` (Avo-generated) path is selected at init by CPU feature detection, with a
differential test asserting the assembly path equals the reference on random inputs plus a
fuzzer sharing that corpus.

**Honest framing of the payoff.** SIMD is the last ~4×; layout is the first ~50×. The
prototype does a `map[uint32]*seriesSamples` lookup and two `proto.Col*.Row(i)` interface
calls *per sample* — order 50–100 ns/sample. A contiguous branch-free `[]float64` loop is
order 1 ns/sample before any vector instruction is involved. The design's job is to make the
kernel shape *reachable*; the assembly is an optimization on top, and the pure-Go path must
stay competitive because it is what runs on arm64 until we write that too.

Go 1.26 (this module's toolchain) does not reliably auto-vectorize, which is precisely why
kernels are isolated behind a narrow, individually benchmarkable API rather than inlined into
operators.

### 7.1 Batching several series into one kernel call

Many series accumulate into the same group row, so a natural question is whether to fuse them:
`AddN(dst, col₁…col_K)` instead of K calls to `AddF64(dst, dst, col_i)`. The answer is yes,
but for a different reason than it first appears, and the win is narrower than it looks.

**It does not buy vector width.** Vector width is already saturated by the *step* axis: a
column is `stepsInChunk` contiguous float64s, and at 240 steps that is 30 AVX2 vectors of
useful work per call. Batching series adds a second axis to iterate, not more lanes.

**It buys memory traffic.** Each `AddF64(dst, dst, src)` costs 2N reads + N writes for N useful
adds — a 3:1 ratio. A fused K-way accumulate costs `(K+2)N` for `KN` adds, so the ratio falls
to `3K/(K+2)`: 2× fewer memory operations at K=4, 2.4× at K=8, approaching 3×.

**But the win is self-limiting**, and this is the part worth writing down. The two regimes are
inversely correlated:

| Regime | Series per group (K) | Accumulator size | Effect |
|---|---|---|---|
| `sum by (job)`, 10 groups | high (~100) | `10 × 240 × 8B` = 19 KB — L1-resident | dst traffic is already L1; fusing saves little |
| `sum by (instance)`, groups ≈ series | K ≈ 1 | huge, thrashes cache | nothing to fuse |

Where K is large enough for fusion to matter, the destination row is small enough to stay in
L1 across all K accumulations anyway. Where the accumulator thrashes, K ≈ 1. So the
locality argument for series batching largely cancels itself, and the §4.8 caution about
high-cardinality grouping is not fixable by fusing.

**Where it genuinely matters: small N.** For an instant query `stepsInChunk` is 1, so
step-axis SIMD yields *zero* vector width — a column is one float. There, batching series is
the only route to vectorization: gather K series' single values into a contiguous K-vector and
reduce that. This is the principled form of the hand-wave in §4.8, and it scopes the work
precisely — implement `AddN` and friends for the small-N path, not as a general replacement.

**Group adjacency is required, and is obtainable.** Storage delivers series in postings order,
not grouped. But every input series' group is computable from its labels at *plan* time
(§3.3), so the operator can bucket arriving columns into per-group pending slots and flush a
slot when it holds K. The buffer must be bounded (`numGroups × K` columns is fine at 10 groups,
not at 10k), with greedy flushing under a byte cap. Only worth building on the small-N path,
per above.

### 7.2 Kernel-level wins with better returns

Two that beat series batching on the range-query path, both exploiting the step axis we
already have:

- **Prefix sums for window folds.** `sum_over_time`/`count_over_time`/`avg_over_time` at every
  step currently re-walk each window. Computing one prefix-sum scan over the series' samples
  (vectorizable) turns each step's window into `P[j] - P[i]` — O(1) per step instead of
  O(window). For a 5m window at 15s scrape that is ~20× less arithmetic. Does not apply to
  `rate`/`increase` (counter-reset handling is not a prefix-sum), nor to `min`/`max`
  (monotonic-deque instead, still O(1) amortized).
- **Fusing unary chains.** `abs(clamp_max(x, 5))` as one pass over the column rather than two,
  keeping the column in registers between operations.

Both are pure kernel work behind the existing interface, and both are measured in M6 before
any assembly is written.

## 7.1 Tracing

The engine emits OpenTelemetry spans through `Opts.TracerProvider` (nil selects the global
provider, as `internal/logql/logqlengine` does). The tracer rides on `EvalContext` — the state
every operator in one evaluation already shares — so no constructor had to grow a parameter, and
a nil tracer is legal so test doubles and embedders need not care.

| Span | Emitted by | Carries |
|---|---|---|
| `scarecrow.Exec` | every query | query text, start/end/step, step count, instant flag |
| `scarecrow.Chunk` | chunked range queries only (§4.4) | chunk index, chunk count, this chunk's steps |
| `scarecrow.Plan` | per chunk | the planned tree (`root.String()`), resolved series count |
| `scarecrow.Series` / `scarecrow.Scan` | vector selectors | selector, window, series count |
| `scarecrow.AggregateGrid` | the grid pushdown | steps, step width, window width, series count |
| `scarecrow.*.PerWindow` | the per-window pushdown fallbacks | **call count**, function/grouping label |

`scarecrow.Plan` deliberately spans schema resolution as well as planning: schemas resolve eagerly
(§3.3), so every data-dependent operator — the pushdowns, `quantile`, `topk`, `count_values` —
does all of its storage work inside it rather than during `Next`.

The `PerWindow` spans exist for one reason, and it is worth stating because it shapes what the
attribute has to be. The per-step pushdown blowup in §10's M5 note was diagnosed by A/B-ing query
shapes against a live deployment, because the engine emitted no spans at all — and even with
spans, a single aggregate duration would have read as "storage is slow". What names the bug is the
**number of calls**, so that is an attribute rather than something to be inferred from counting
sibling spans, which sampling may well have dropped. `TestTracingShowsPerWindowCallCount` pins it.

## 8. Correctness strategy

Compliance is not a phase at the end; it is the first thing wired up.

1. **Upstream suite, day one.** `promqltest.RunBuiltinTests(t, engine)` runs Prometheus' own
   PromQL test corpus against anything implementing `promql.QueryEngine`. It drives the engine
   through a `storage.Queryable`, so the engine needs a `queryableScanner` adapter (row →
   columnar, one copy) used *only* here and as a fallback. That adapter is ~150 lines and it
   unlocks the entire upstream corpus with zero storage plumbing. **Build it in M0 and track
   pass count as the project's headline metric.**
2. **Real backend.** `promqltest.RunBuiltinTestsWithStorage(t, engine, newStorage)` with
   `newStorage` returning a `storagebackend`-backed store — same corpus, real fetch seam,
   catching everything the adapter hides (pooling, release, sharding, pushdowns).
3. **Differential fuzz** against `promql.NewEngine` over a shared in-memory dataset, seeded
   from the prototype's query corpus (§9).
4. **Golden benchmarks** following the existing `internal/storagebackend/goldenbench_*_test.go`
   pattern, measured against the Thanos fork on the same data.
5. **Compliance suites** in `dev/local/ch-compliance` as the final gate.

### 8.1 Where the corpus stands

The file-level pass count (6/21 after M5) is the number milestones are judged by, but it is a
harsh metric: one unimplemented function fails a 413-case file, and the skip list then hides the
other 412 cases. `TestPromQLGap` runs the corpus unskipped — promqltest makes every `eval` its
own subtest and keeps going after a failure — which gives the finer number: **1016/2117 eval
cases (48.0%)**.

Attributing the 1101 failures:

| Cause | Cases | Milestone |
|---|---:|---|
| Native histograms (`histogram_*`, histogram operands and expectations, including `count_values` over a mixed float/histogram series) | ~700 | M7 |
| Missing annotations (`info`/`warn` expected, none produced) | 117 | M9 |
| Extended range selectors (`anchored`/`smoothed`) | 74 | — |
| Binop fill modifiers | 39 | — |
| `info()` | 41 | — |
| Created timestamps | 16 | M10 |
| `__name__` handling (delayed removal, `type`/`unit` metadata labels) | 23 | M8 |

`topk`/`bottomk`/`quantile`/`limitk`/`limit_ratio`/`sort*` are gone from this table: M2b
implemented all of them, closing 58 cases outright. The rest of what that row used to cover
turned out to already be double-counted under the rows above — a `limitk`/`topk` case over a
native-histogram series was always going to fail on M7 regardless of M2b, and `sort()`'s "expect
warn" cases were always an M9 gap, not a sorting bug.

Date/time functions and the `predict_linear`/`deriv`/`quantile_over_time`/`mad_over_time`/
`ts_of_*` row are gone the same way: M12 implemented all of them, closing 75 cases. The
~90-vs-75 gap between what those two rows used to estimate and what actually closed is the same
double-counting pattern — a `quantile_over_time` or `predict_linear` case over a mixed
float/histogram series was always an M7 failure regardless of M12, and the invalid-quantile
"expect warn" cases were always M9.

`absent`/`absent_over_time` is gone from this table too: M13 closed 38 of the 42 cases the row
used to estimate. The remaining 4 are the same double-counting pattern once more — an `absent`
case wrapping a native-histogram selector, always an M7 failure regardless of M13.

The shape of that table is the useful part: **almost everything left is a missing feature, not a
wrong answer.** Outside the two native-histogram files there are 90 wrong-answer failures, and
all but a handful belong to features that are absent rather than broken — 39 are the fill
modifiers, 16 created timestamps, 15 `type`/`unit` metadata. What that says is that the execution
model is carrying the semantics correctly and the remaining work is breadth, which is the
cheerful reading of a 48.0% number.

## 9. Salvage from `gemini/scarecrow-engine-initial`

The branch predates the `go-faster/oteldb` → `oteldb/oteldb` module rename, so nothing
cherry-picks cleanly; salvage is file-by-file copy plus import rewrite.

**Keep — port as-is or nearly.**

| What | Why |
|---|---|
| `internal/promql/testdata/corpus/*` (~1270 lines of queries) | Highest-value artifact on the branch. Repurpose as the differential-fuzz seed corpus and the benchmark query set. Survives the parser decision completely. |
| `scarecrow/binop.go`: `hashMatchingKey`, `propagateLabels`, one-to-many/many-to-one detection | The fiddliest logic in PromQL, and it is *schema-level* — it moves into planning almost unchanged. |
| `scarecrow/range_agg.go`: `instantValue`, `countChanges`, `countResets`, `aggregateOverTime` | Correct scalar math, already slice-shaped. Becomes kernels. |
| Two-pointer sliding-window walk (in `StepEvaluator` and `RangeFunctionOperator`) | Correct and O(samples + steps). Reimplement over one series' `Samples`, writing straight into that series' `Column`. |
| `agg.go`'s `quantile`, aggregation semantics | Reference for the per-step reductions. |
| `binop_test.go`, `grouping_test.go`, `range_agg_test.go`, `engine_test.go` (~1450 lines) | Real behavioural tests, cheap to re-point at the new API. Best salvage after the corpus. |

**Drop.**

| What | Why |
|---|---|
| `internal/promql/lexer`, `parser.go`, `expr.go` (~1100 lines) | Superseded by the upstream parser. |
| `Block` (`proto.Col*`) | The chstorage coupling this design exists to remove. |
| `SharedOperator` / `sharedCursor` | Unbounded buffering; replaced by CSE + bounded `Tee`. |
| `map[uint32]*seriesSamples` materialization in every operator | The core performance defect. |
| Per-operator `errgroup` | Replaced by the single `Concurrent` operator. |
| `zctx...Debugf("DEBUG: ...")` on hot paths | Replaced by an `Explain` mode and sampled per-operator tracing spans. |
| `chstorage/querier_scarecrow.go`, `promhandler/scarecrow.go`, `scarecrow-*.yml` | Out of scope per the chstorage-free decision. |

**Fix during port.** `extrapolatedRate` in `range_agg.go` is wrong — its own comment concedes
it ("Prometheus uses more complex extrapolation"). It omits Prometheus' boundary extrapolation
and clamping entirely, so `rate`/`increase` are off near window edges and `increase` on a
short window is badly wrong. Port from upstream `promql/functions.go` rather than fixing in
place.

## 10. Milestones

Each milestone ends with a number: upstream `promqltest` files passing.

- **M0 — skeleton.** *Done.* `Column`, `Schema`, `Operator`, `kernel` package, `queryableScanner`,
  engine implementing `promql.QueryEngine`, `promqltest` harness wired and failing loudly with
  a tracked pass count.
- **M1 — read path.** *Done.* Vector selector, fused matrix selector + fold, lookback and
  staleness, offset, `rate`/`increase`/`delta`/`irate`/`idelta`/`changes`/`resets` and the
  `*_over_time` family, `ScaleFactors` per the §3.5 matrix.

  Two things the corpus forced that this plan had put in M3. **`@` had to ship in M1**:
  promqltest rewrites every instant query as `expr @ <ts>`, so no corpus file passes without
  it. It cost little — `promql.PreprocessExpr` resolves `start()`/`end()`, and a pinned
  timestamp is just a constant `refTime` per step. And a **bare range selector as an instant
  query** (`some_metric[1m]`) returns a raw matrix, the one result shape no operator can
  produce; it is materialized at the result boundary rather than bending the rule that nothing
  emits a range vector.

  Correctness rests on a **differential test against the upstream engine** (29 queries × 5
  instants + × 3 step sizes), not on hand-computed expectations — that is what pins rate
  extrapolation and the left-open window boundaries. `ScaleFactors` cannot be reached that way
  (the Prometheus storage interface has no weight channel), so the §3.5 matrix is asserted
  row-by-row against a fake weighted scanner instead.
- **M2 — expressions.** *Done* (the full-set aggregations it deferred are M2b, also done). Aggregations (`sum`/`min`/`max`/`avg`/`count`/`group`/
  `stddev`/`stdvar`), all binary operators including vector matching and set operators, and the
  instant functions (unary math, `clamp*`, `round`, `timestamp`, `scalar`, `vector`,
  `label_replace`, `label_join`, `pi`). Corpus: 5/21 files, ~370 cases.

  **Deferred within M2**, because each needs the full per-step series set, which the accumulator
  shape does not give: `topk`/`bottomk` (a per-step bounded heap, §4.4), `quantile`
  (`O(series × steps)`, inherent), `sort`/`sort_by_label`, `limitk`, and `count_values`. All are
  done now (M2b, and `count_values` alongside it — see below).

  Three upstream behaviours worth recording, each found by the corpus rather than by reading:
  a comparison keeps the **vector's** value even when the scalar is the left operand; `sum` and
  `avg` need **Kahan compensated summation**, with `avg` switching from a direct mean to an
  incremental one only once the running sum would overflow; and `or` can map two inputs onto one
  output identity (`-a or -b` both reduce to `{}`), so it must merge them into one column rather
  than emit two.

  Also added: PromQL's **duplicate-labelset error**. Operators that drop `__name__` can collapse
  distinct inputs onto one identity, and returning both silently is wrong.

- **M2b — full-set aggregations.** *Done.* The operators M2 deferred, all of which need every
  series at a step rather than an incremental fold: `topk`/`bottomk` (`limitAgg`, a per-step
  bounded heap kept as a linear worst-scan over an array rather than `container/heap` — both are
  correct top/bottom-k selection algorithms and a heap's root is always the current worst by
  construction, so scanning for it here yields the identical survivor set; `O(k × steps)` per
  group), `quantile` (`quantileAgg`, `O(series × steps)`, inherent — upstream pays it too),
  `sort`/`sort_desc`/`sort_by_label`/`sort_by_label_desc` (`sortOp`, instant-only, so it ranks by
  the child's first step), and the `limitk`/`limit_ratio` family (`limitAgg` again — `limit_ratio`
  admits by a deterministic per-series label-hash offset, not by value, so it needs no heap at
  all).

  **`count_values`** shipped separately from the rest of M2b, once the pattern above proved out:
  `countValuesAgg` groups by (grouping labels, observed value) and counts per group, per step.
  Its schema is data-dependent in a stronger sense than `limitAgg`'s, though — a survivor set is
  a *subset* of the input's own label sets, known at plan time even if which subset isn't; a
  `count_values` group's labels are *synthesized* from a value, an identity that exists nowhere
  until the data is read, and can differ for the same input series from one step to the next.
  Despite that, the implementation is the same shape as every other operator here: drain the
  child once inside `Schema()`, keying each (series, step) pair's output label set by its string
  form so a repeat occurrence reuses the row rather than duplicating it. Open question 4
  originally called this out for special treatment; in practice it needed none.

  `topk`/`bottomk` additionally have to reproduce upstream's *output order* — descending or
  ascending by value — which only an instant query (one step) can observe, since a range query
  merges steps by series identity regardless of emission order. `limitAgg` resolves this by
  ordering each group's survivors once, from their first appearance across the whole grid, rather
  than threading order through the per-step heap itself.

  Building this surfaced one unrelated latent bug: a degenerate subquery grid (step wider than
  range, missing every aligned tick in the window) used to hit `buildSubquery`'s "empty grid"
  guard and fail the query outright. That guard predates M2b, but nothing in the corpus reached
  it until a `topk` inside such a subquery did. An empty grid is a legitimate "no sample" result,
  not an error, so the guard is gone and `vectorSelect` now short-circuits to an empty schema
  over zero steps instead of indexing into an empty timestamp slice.

  Gate: `aggregators.test`, and the `sort` cases in `range_queries.test`. Neither file passes
  outright yet — both still carry unrelated M7/M9 gaps — but the M2b-attributed failures in them
  are gone; see §8.1.
- **M3 — subqueries.** *Done.* (`@` and `offset` landed in M1, forced by the corpus.) A subquery
  plans its inner expression against its own step grid — aligned to the subquery step rather than
  to the outer query's start, as upstream does — and the results become a fold's samples.

  This is where series-major pays off a second time: the inner operator already emits one column
  per series, and a column *is* that series' samples along the step axis, so the conversion is a
  direct read, one series at a time, with no transpose and no materialization of the inner result
  set. The step-major draft needed a dedicated `Transpose` operator here (§3.2); it was never
  written because it is not needed.

  The refactor introduced a `foldSource` seam so a selector and a subquery feed the *same* fold
  machinery — the only difference is where the samples come from. Every subquery case in
  `subquery.test` passes; the three that do not are `topk` (M2b) and native histograms (M7).
- **M4 — parallelism.** *Partly done.* `Concurrent` is implemented and wired into both sides of
  every vector binary operator, which is where two independent subtrees each reach storage.

  **The producer starts during schema resolution, not on first `Next`.** That detail is the
  whole mechanism: a vector binop drains its build side to completion before it ever calls
  `Next` on the streaming side, so a producer started lazily would not begin until the other had
  finished — exactly the serialization the operator exists to remove. Schemas resolve bottom-up
  for the whole tree before execution (§3.3), so starting there makes every producer run ahead
  concurrently. A failed plan closes the tree so nothing started is leaked.

  The semaphore is **try-only**: an operator that cannot get a slot runs its child inline rather
  than waiting, so contention costs parallelism and never liveness. That is what keeps the
  deadlock-freedom claim true under a bounded limiter.

  **Still to do in M4:** series sharding as map-reduce over accumulators, and CSE with a bounded
  `Tee`. Sharding is deliberately deferred until M5 wires the columnar seam — over a
  `storage.Queryable` each shard would re-scan every series and filter, parallelizing CPU while
  multiplying I/O, which is the wrong trade to bake in.

  **CSE, and why the `Tee` must be allowed to give up.** `a / sum(a)`, `rate(x[5m]) /
  sum(rate(x[5m]))` and every ratio-of-itself scrape a subexpression twice. The fix is to plan it
  once and fan its columns out to both consumers through a `Tee`.

  Key on the *scan request* — matchers, window, and for a fold its range/offset/`@` — not on the
  expression string, so two spellings of the same read share and two reads that merely look alike
  do not. This is what the Thanos fork's `SelectorPool` does, keying `hash(matchers, mint, maxt,
  hints)`; note it dedups *identical* selectors only, and never merges different ones into a union
  query.

  The hard part is not the sharing, it is the buffering, and it collides with the fact recorded
  above: **a vector binop drains its build side completely before calling `Next` on the streaming
  side.** So if both sides read one `Tee`, one consumer runs to the end while the other has not
  started. A `Tee` bounded at *k* columns must then either block the fast consumer — deadlock,
  since the slow one cannot start until the fast one finishes — or buffer without limit, which
  reproduces exactly the `coalesce.loadSeries` behaviour of #1116 that this engine exists to
  remove.

  The resolution follows the rule the `Concurrent` semaphore already established: *degrade, never
  block.* A `Tee` buffers up to its bound; if a consumer would exceed it, that consumer falls back
  to re-executing the subexpression against storage instead of waiting. CSE then becomes a
  best-effort optimization that is free when the consumers travel together and costs a second scan
  when they do not — and it can never deadlock, which is the property worth more than the sharing.

  This also sets the honest expectation: CSE will *not* help the common `a / sum(a)` shape, because
  a binop is precisely the operator that separates its consumers in time. It pays where consumers
  advance in lockstep — several aggregations over one selector, the many-side of a join feeding two
  operators — and the bound is what keeps the other case from silently becoming a memory problem.
- **M5 — pushdowns.** *Done.* The three pushdowns are planner rules over optional `Scanner`
  capabilities — `AggregateScanner` (reducer `*_over_time`), `SeriesCounter` (`count`),
  `GroupedSeriesCounter` (`count by (l)`) — and `internal/storagebackend`'s `ScarecrowScanner`
  now implements all three, reusing the same `AggregateMetricsNamed` and
  `storagepromql.Queryable`-backed `CountSeries`/`CountSeriesBy` calls the fork engine's own
  pushdowns use. A scanner that implements none of them answers every query identically, only
  slower, which is the property the tests assert rather than assume — verified here with a
  differential oracle against the fork engine over `sum_over_time`, `count`, and `count by`.

  **A pushdown that is not grid-aware is not an optimization.** All three capabilities above take
  a single window, so a range query calls them once per step. That reads as cheap — an index
  lookup, no samples decoded — and is catastrophic in practice: measured against a live
  deployment, `count by (cpu)` over a 1h/15s grid (241 steps) took **11.9 s** through the
  per-step path against **0.04 s** for the same query with no pushdown at all, and scaled
  linearly in step count (13 steps → 0.81 s, 61 → 2.63 s). That is ~49 ms of fixed cost per step
  — a fresh querier per call, and a storage-side fallback that re-fetches every matching series
  — where the naive path fetches the window once and counts in the engine. The "optimization" was
  ~240× slower than doing nothing.

  `GridAggregateScanner` fixes this with one call for the whole grid, implemented over storage's
  `AggregateMetricsWindowNamed` — the same windowed call the fork engine's range `*_over_time`
  pushdown already used and which `count` never got. Storage folds each series' samples once into
  step buckets and slides them into every overlapping window, so cost tracks the data in range
  rather than range/step times it. When present it supersedes all three per-window capabilities;
  instant queries and `@`-pinned grids keep the per-window path, where a single window is exactly
  the right question.

  Two things about that seam are worth stating because both were caught by tests rather than by
  reading. Windows come back **keyed by evaluation timestamp, not by position** — the request
  reaches a full window-width before the first step, so storage legitimately returns windows
  ending before it, and indexing positionally shifts every value onto the wrong step whenever the
  step does not divide the window evenly. And a **single-step grid is not a grid**: synthesizing
  one means inventing a step the query never had, which storage reads as the bucket width it
  folds into, silently truncating the window. Both failures are invisible at step sizes that
  divide evenly, which is why the differential tests run a step that does not.

  Two obligations bind an implementer, and they are why the capabilities are opt-in rather than
  assumed. Windows are PromQL's half-open `(mint, maxt]`, not storage's inclusive range — widening
  produces wrong answers at window edges that the corpus will not reliably catch. And staleness
  markers must not exist in the data: the engine drops them during the fold, and a storage-side
  aggregate cannot, so it would report a stale series as present.

  **The over-time pushdown is the one place result memory scales with the matched series set.**
  Storage answers a window at a time, all series at once; this engine emits a series at a time,
  all steps at once. So every window's result is held until the last lands — `O(series × steps)`,
  against the `O(1)` in series a streaming selector achieves (§4.6). It is still far less than the
  raw samples it replaces (a 5m window at 15s holds 20 samples per series per step against one
  aggregate), but a series-major aggregate API on the storage side would remove it entirely, and
  that is worth asking for.

  Building this also surfaced an undocumented contract: **a `Scanner` must be safe for concurrent
  use.** One scanner serves a whole query and `Concurrent` evaluates both binop subtrees at once,
  so two selectors can call it simultaneously. It was found by `-race` on a test double, which is
  precisely the sort of thing that would otherwise be found in production.

  `internal/storagebackend` now wires `scarecrow.NewEngine` to `Backend.ScarecrowScanner()`
  (opt-in behind `prometheus.enable_scarecrow_engine`, since corpus coverage is still partial —
  see §8.1), which is what closes #1116/#1117 on this path once the flag is on by default.
- **M6 — kernels.** Assembly paths + golden benchmarks vs the fork.
- **M7 — native histograms.** **Blocked on storage:** `fetch.Batch` has no histogram column
  (`Timestamps []int64` + `Values []float64` only) and `signal/metric` has no histogram kind.
  Native histograms are not representable at the seam at all today, so this needs an
  `oteldb/storage` change first, not an engine change.

  Gate: `histograms.test`, `native_histograms.test`, and the histogram cases in
  `operators.test` and `aggregators.test`.

The following three were discovered by the corpus during M1 and M2 rather than planned. Each is
a feature in its own right, not a gap in a milestone above, so each gets its own.

- **M8 — delayed `__name__` removal.** Prometheus 3 does not drop `__name__` at the operator
  that logically removes it; it flags the series (`DropName`) and removes the label only when
  building the result. That is observable: `label_replace(rate(x[5m]), "n", "$1", "__name__",
  "(.+)")` can still read `__name__` even though `rate` "dropped" it. This engine drops eagerly,
  which is simpler and right for every case *except* a later reader of `__name__`.

  Implementing it means a per-series drop flag travelling with the column and applied in
  `collect`, plus every `dropMetricName` call site becoming a flag set. Touches selectors,
  `matrixFold`, aggregations, binops and the instant functions — wide but shallow.

  Gate: `name_label_dropping.test`.

  **`info()` belongs here.** It joins a series against the `target_info` metric and merges the
  matched labels in, which is the same identity-rewriting machinery from the other end: M8 defers
  a label's removal, `info` adds labels a series never carried. Both need identity to be
  something a column can carry rather than something frozen at plan time, so doing them together
  costs less than doing either alone. Gate: `info.test` (41 cases).

- **M9 — annotations.** `promql.Result.Warnings` is always empty today. PromQL emits info- and
  warn-level annotations (a histogram ignored in a `stdvar`, a mixed float/histogram `rate`, a
  range too short) and the corpus asserts them with `expect info` / `expect warn`. Needs an
  annotation sink threaded through evaluation and returned from `Exec`.

  Gate: the `expect info` cases in `aggregators.test`; a prerequisite for closing out M7.

- **M10 — created timestamps.** Prometheus 3 carries a per-sample start timestamp
  (`chunkenc.Iterator.AtST`) and uses it to detect a counter reset that a value comparison alone
  would miss. `Samples` has no field for it and `queryableScanner` does not read it. Whether
  `oteldb/storage` can supply one is an open question — like native histograms, this may be a
  storage change before it is an engine change.

  Gate: `start_timestamps.test`.

- **M11 — resource limits.** *Done.* `Opts.MaxSamples` and `Opts.Timeout`, mirroring the
  upstream engine's so `prometheus.max_samples` and `prometheus.timeout` keep working when
  `enable_scarecrow_engine` is on. Failures are upstream's own `promql.ErrTooManySamples` and
  `promql.ErrQueryTimeout`, so the HTTP status mapping and the error text are identical between
  engines. Zero means unlimited, which is where scarecrow deliberately differs: upstream treats
  a zero value as "fail every query", a footgun for an embedder that simply did not set it.

  The count is cumulative over the query rather than a live high-water mark. The columnar model
  holds one series' raw samples at a time (§3.2), so a peak gauge would never trip on the shape
  worth stopping — a scan touching millions of series — and would only ever measure the output
  grid. One budget covers the whole query, so M16's chunking cannot be used to read past it.

  Charged at every point data enters the engine: the two selector leaves, the raw-matrix
  collector, and the pushdowns. That last one matters most in production: a pushed-down query
  reads no raw samples at all, so charging only the leaves would exempt exactly the large
  queries the pushdown exists for.

  **What it does not bound:** the work storage does. A pushdown is charged for the values it
  returns, not the samples scanned to produce them — `count(x)` over a million series charges
  one value per step. That limit belongs where the counts are known; see
  [oteldb/storage#263](https://github.com/oteldb/storage/issues/263).

  No corpus gate — this is about what happens past the corpus' scale.

The next four come from §8.1, and from a check the plan had never run: enumerating the function
surface and asking what has no owner. M0–M7 were designed from the architecture and M8–M10 from
what the corpus surfaced while building M1 and M2, so between them they cover everything hard.
What they missed is a long tail of features that are mostly easy — 25 of the 37 unimplemented
functions, plus two pieces of syntax, worth ~286 corpus cases or **22% of all remaining
failures**. Cheap and unowned is how work stays undone indefinitely, so it gets milestones.

- **M12 — the function tail.** *Done.* Three groups, all of which dropped into tables that
  already existed:

  - **Date/time** — `time`, `year`, `month`, `hour`, `minute`, `day_of_month`, `day_of_week`,
    `day_of_year`, `days_in_month`. With an argument they are `unaryFn` plus a
    unix-seconds-to-`time.Time` adapter (`datefn.go`); with none, a new tiny operator,
    `stepDateFn`, since nothing existing emits a value that is a pure function of the step
    timestamp with no input column at all — `numberLiteral` is constant across steps, and
    `time()`'s whole point is that it isn't. 41 cases.
  - **Range functions** — `deriv`, `predict_linear`, `quantile_over_time`, `mad_over_time`. All
    four are new entries in `rangeFuncs`, but `quantile_over_time` and `predict_linear` also
    needed a real capability the type didn't have: a second, per-step scalar argument. `rangeFunc`
    grew a `param float64` parameter (every existing entry now ignores it), and `matrixFold`
    grew an optional `param Operator`, evaluated once via the same `scalarValues` helper
    [`quantileAgg`]/[`limitAgg`] already used for `quantile`/`topk`'s per-step k. The two
    functions disagree on argument order — `quantile_over_time(q, matrix)` vs.
    `predict_linear(matrix, t)` — so `buildCall` resolves which argument is which by name rather
    than by position. `predict_linear` also needed the window to carry its own step's *un-offset*
    evaluation timestamp (`window.EvalMs`) separately from `RangeEnd` (which offset shifts):
    upstream anchors the regression at the query's own step time regardless of what `offset`
    shifted the sample window to, so a naive reuse of `RangeEnd` would answer at the wrong point
    for any query using both `predict_linear` and `offset` together — caught by
    `functiontail_test.go`'s `predict_linear(counter[1m] offset 10s, 3600)` case, which pins
    exactly that combination against the upstream engine. `predict_linear` and `deriv` matter
    beyond the corpus: alerting rules lean on them. 34 cases.
  - **Timestamp-of** — `ts_of_first_over_time`, `ts_of_last_over_time`, `ts_of_max_over_time`,
    `ts_of_min_over_time`. Plain `rangeFunc` entries; `ts_of_max_over_time`/`ts_of_min_over_time`
    replicate upstream's tie-break exactly (`>=`/`<=`, not a strict inequality — ties resolve to
    the *latest* matching sample, not the first). The query-context accessors
    (`start`/`end`/`step`/`range`) turned out to need **no engine code at all**: upstream's
    `promql.PreprocessExpr` — already called once per query, against the whole query's own
    start/end/interval, specifically so `@ start()`/`@ end()` resolve correctly ahead of M16's
    chunking — rewrites bare `start()`/`end()`/`step()`/`range()` calls into plain
    `NumberLiteral` nodes before the planner ever sees them. 11 cases; the accessors have none at
    all, which is the only reason they looked unimplemented.

  Verified by a differential suite (`functiontail_test.go`) against the upstream engine, at
  several instants and step sizes, in addition to the corpus. Gate: the corresponding cases in
  `functions.test` — the file itself still doesn't pass outright, gated on unrelated M7/M9 gaps
  (native histograms, warning/info annotations) and one unrelated function
  (`double_exponential_smoothing`) this milestone never covered.

- **M13 — `absent` / `absent_over_time`.** *Done.* Small in surface, awkward in shape: both must
  emit a series precisely when the input produced none, and "no columns" is exactly how a
  streaming operator signals it is finished (`present`/`absent_over_time` in §4.4's table).
  So this is not a per-series fold — it is one operator (`absentOp`) that drains its child
  entirely, OR-reduces presence across every input series per step, and emits a single series at
  the steps where that OR came back false. Its identity is synthesized from the argument's own
  label *matchers* rather than from any observed series (`createLabelsForAbsentFunction`, a
  faithful port of upstream's, including the "same label used twice as an equality matcher drops
  it" backwards-compatibility quirk) — the sole scarecrow operator whose schema comes from AST
  syntax rather than data.

  `absent()`'s argument is a general vector expression, so it just wraps whatever operator
  `p.build` already produces for it. `absent_over_time()` is the one that doesn't fit the
  `rangeFunc`/`matrixFold` contract at all: `matrixFold.fold` skips calling the range function
  precisely when a step's window is empty, which is the one condition `absent_over_time` needs to
  observe. It is planned instead as `presentOverTime` — an ordinary range function, still eligible
  for the same `AggregateScanner` pushdown every other `*_over_time` reducer gets — wrapped in the
  same `absentOp`, so the OR-reduce-and-invert trick does double duty for both functions. Verified
  against the upstream engine via `differential_test.go`, including the duplicate-matcher and
  non-selector-argument edge cases. Closed 38 of the corpus's 42 cases; the remaining 4 wrap a
  native-histogram selector and are an M7 gap, not an M13 one.

- **M14 — extended range selectors.** The `anchored` and `smoothed` modifiers on a range
  selector, which change how the window's endpoints are chosen — `smoothed` interpolates at the
  boundary rather than taking the samples as they fall. They apply only to a fixed set of
  functions (`rate`, `increase`, `delta`, and for `anchored` also `changes`/`resets`), and
  upstream errors on any other pairing, so the planner must reject those combinations by name.
  Lands in `matrixFold`, where the window is already built. Gate: `extended_vectors.test`
  (74 cases).

- **M15 — binop fill modifiers.** `fill(x)`, `fill_left(x)`, `fill_right(x)` on a vector binop
  supply a default for series present on one side only, which turns the join from an inner join
  into an outer one. That is a real change to `vectorBinop`: the streaming side can no longer just
  skip an unmatched row, and the build side must emit its unprobed rows at the end — which is
  what `vectorBinop`'s existing `probed []bool` already tracks. Gate: `fill-modifier.test`
  (39 cases).

The last one is numbered last and should be built early. It is the only milestone here that
closes a memory problem rather than adding a capability, and three separate operators are waiting
on it.

- **M16 — time-chunking.** *Done.* Split a long range into sequential chunks of `Opts.ChunkSteps`
  steps (default 10,000) and re-run the whole pipeline per chunk, concatenating results by series
  identity (§4.4). Parts are time-partitioned, so a chunk's fetch touches only its own parts.

  Shipped with a **static, configured** chunk size rather than the dynamic "sized by the planner
  from the widest accumulator in the tree" this milestone was originally scoped as: measuring that
  width would require resolving a data-dependent operator's `Schema()` (`quantile`/`topk`/the
  `aggregateOverTime` pushdown) once to size chunks and again per chunk, which runs the exact
  full-range computation chunking exists to avoid. A fixed budget still delivers the property that
  matters — peak resident flat in range length — and is one field on `Opts` rather than a planner
  pass; the dynamic version is a possible follow-up if `ChunkSteps`'s one-size-fits-all default
  ever proves wrong for a real workload.

  Before this, `stepGrid` built the entire query grid into one `EvalContext`, so **every operator
  whose cost carries a `steps` factor was multiplied by the whole range** — 172,801 steps for
  30 days at 15s, or 1.4 MB per buffered series. Three of them depended on this being bounded: the
  one-to-one vector binop's build side (§4.6), the `aggregateOverTime` pushdown's per-window
  results (M5), and any group-heavy accumulator where groups approach series. Each is individually
  defensible; together they were the same `O(series × steps)` shape the design rejects in the
  fork, and chunking is the one fix that covers all three at once.

  Doing this earlier than its number suggests was the point. It was originally folded into M11 as
  a companion to resource limits, which understated it: a limit *refuses* a query that would use
  too much memory, while chunking lets the same query *succeed*. They pair well but they are not
  the same work, and only one of them makes long-range queries usable.

  No corpus gate — the corpus is entirely short-range, so nothing in §8 exercises this. Verified
  instead by a differential suite (agreement between chunked and unchunked evaluation, and with
  the upstream engine, down to one step per chunk) and a memory-archetype benchmark showing a
  lower peak for a chunked one-to-one join than the same join run unchunked.

## 11. Open questions

1. *(resolved)* **Package name** — `internal/scarecrow`, keeping the prototype's codename and
   staying unambiguous against `internal/promql` while the Thanos wrapper still exists.
2. *(resolved)* **Accumulator cost under high group cardinality** — **chunking plus a hard
   limit.** Time-chunking bounds the accumulator (§4.4); past a configured budget the query
   fails with a clear error rather than consuming the process. This is the same posture as
   Prometheus' `MaxSamples`, and it is the right one because a query whose accumulator is
   `O(series × steps)` has a result that large *anyway* — spilling to disk would buy a
   completed query whose response body is the real problem, at the cost of real machinery
   (encoding, temp-file lifecycle, cleanup) for what is usually a mistaken `by (instance)`.
   Failing fast and legibly beats succeeding slowly. Implemented; see M11.
3. *(tracked elsewhere)* **Delta-temporality `rate`** — [#1190](https://github.com/oteldb/oteldb/issues/1190).
   Investigating this turned up something larger than the sampling question: oteldb performs no
   delta→cumulative conversion anywhere, so delta-temporality sums reach PromQL as-is, and
   `rate()` over them is already meaningless independent of sampling. The §3.5 matrix therefore
   specifies only the cumulative rule, which is well-defined; the delta branch cannot be written
   until that issue is answered, because it presupposes a semantics that does not exist. This is
   an ingest-and-storage question, not an engine one.
4. *(superseded)* **`count_values`** — originally resolved as **stays unsupported**, on the
   premise that supporting it meant weakening §3.3's invariant globally. Revisited once M2b's
   `quantileAgg`/`limitAgg` shipped: they *already* drain their child inside `Schema()` to answer
   a data-dependent question (which series survive `topk`, what the exact quantile is), so the
   invariant was never a hard wall — `count_values` is now `countValuesAgg`, one more member of
   that same "eager `Schema()`" family rather than a special case. Implemented; see M2's note.
