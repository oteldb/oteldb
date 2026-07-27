# Native PromQL engine

Design for a native, columnar, batched PromQL execution engine over the `oteldb/storage`
fetch seam, replacing the `github.com/oteldb/promql-engine` (Thanos) fork on the
`internal/storagebackend` path.

Status: implemented through M3 (selectors, range functions, aggregations, binary operators,
instant functions, subqueries) in `internal/scarecrow`. Codename from the
`gemini/scarecrow-engine-initial` prototype. Milestones and their gates are in §10.

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

**Chunking.** Long ranges are still split into sequential chunks so that the largest
accumulator stays under budget — `sum by (instance)` where groups ≈ series is genuinely
`O(series × steps)`, and 30 days at 15s is 172k steps. The planner sizes `stepsInChunk` from
the widest accumulator in the tree and re-runs the pipeline per chunk, concatenating results.
Parts are time-partitioned, so a chunk's fetch touches only its own parts and decodes nothing
extra. This is now a policy knob for a minority of queries rather than the load-bearing
mechanism it was under step-major.

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

The `O(series)` term is gone entirely: nothing in the engine is proportional to input series
cardinality, because input series are consumed one at a time and released. That is a better
asymptotic than the cursor model, not merely a recovery from the step-major regression.

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

The fan-out is the obstacle, and not for the reason the phrase "merges by `signal.SeriesID`"
suggests. `query/fetch.Merge` is **not** an ordered merge: `mergeFetcher.Fetch` `Drain`s every
child completely, hash-groups the batches into a `map[signal.SeriesID]*mergeAcc` ordered by
*first appearance*, deep-copies, and returns a slice iterator. So there is no k-way merge to
parameterize with a comparator — it would have to be built.

Sorting by `signal.SeriesID` also would not serve the join even if it existed. The join matches
on **signature**, a label subset; two series that must join carry different labels (`__name__`
differs at minimum) and therefore different content-addressed ids, so id order and signature
order are unrelated.

**A finding worth separating from this rule**, filed as
[oteldb/storage#208](https://github.com/oteldb/storage/issues/208): because `Merge` drains and
materializes, the engine's `O(1)`-in-series raw level (§4.3) holds for a single-node fetcher but
**not behind a fan-out** — cluster and multi-tenant reads buffer every matched series' samples,
then copy them. The engine cannot recover the property from above, since the materialization has
already happened by the time it sees the iterator.

Making `Merge` a streaming k-way merge ordered by id fixes that on its own merits, and it is
cheaper than it sounds: `head.resolve` returns an intersection of the postings index's sorted
series lists, so every child already emits in ascending `signal.SeriesID` order and needs no
change. It would also leave exactly the ordered-merge machinery a caller-specified key needs —
which is why that ordering is this rule's prerequisite rather than the rule itself.

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

The file-level pass count (6/21 after M4) is the number milestones are judged by, but it is a
harsh metric: one unimplemented function fails a 413-case file, and the skip list then hides the
other 412 cases. `TestPromQLGap` runs the corpus unskipped — promqltest makes every `eval` its
own subtest and keeps going after a failure — which gives the finer number: **844/2117 eval cases
(39.9%)**.

Attributing the 1273 failures:

| Cause | Cases | Milestone |
|---|---:|---|
| Native histograms (`histogram_*`, histogram operands and expectations) | ~700 | M7 |
| Missing annotations (`info`/`warn` expected, none produced) | 117 | M9 |
| `topk`/`bottomk`/`quantile`/`limitk`/`limit_ratio`/`count_values`/`sort*` | 148 | M2b |
| Extended range selectors (`anchored`/`smoothed`) | 74 | — |
| `absent`, `absent_over_time` | 42 | — |
| Date/time functions (`year`, `month`, `hour`, `minute`, `time`, …) | ~45 | — |
| `predict_linear`, `deriv`, `quantile_over_time`, `mad_over_time`, `ts_of_*`, … | ~45 | — |
| Binop fill modifiers | 39 | — |
| `info()` | 41 | — |
| Created timestamps | 16 | M10 |
| `__name__` handling (delayed removal, `type`/`unit` metadata labels) | 23 | M8 |

The shape of that table is the useful part: **almost everything left is a missing feature, not a
wrong answer.** Outside the two native-histogram files there are 128 wrong-answer failures, and
all but a handful belong to features that are absent rather than broken — 39 are the fill
modifiers, 16 created timestamps, 15 `type`/`unit` metadata. What that says is that the execution
model is carrying the semantics correctly and the remaining work is breadth, which is the
cheerful reading of a 39.9% number.

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
- **M2 — expressions.** *Done, except what M2b covers.* Aggregations (`sum`/`min`/`max`/`avg`/`count`/`group`/
  `stddev`/`stdvar`), all binary operators including vector matching and set operators, and the
  instant functions (unary math, `clamp*`, `round`, `timestamp`, `scalar`, `vector`,
  `label_replace`, `label_join`, `pi`). Corpus: 5/21 files, ~370 cases.

  **Deferred within M2**, because each needs the full per-step series set, which the accumulator
  shape does not give: `topk`/`bottomk` (a per-step bounded heap, §4.4), `quantile`
  (`O(series × steps)`, inherent), `sort`/`sort_by_label`, and the `limitk` family.
  **`count_values` is worse than deferred** — its output series are labelled by the observed
  *values*, so its schema is data-dependent and cannot be resolved at plan time. That conflicts
  with §3.3's invariant. The invariant could be weakened from "resolved at plan time" to
  "resolved before this operator emits its first column", which preserves ref stability; that is
  a design decision, not an implementation detail, so it is deliberately not made here.

  Three upstream behaviours worth recording, each found by the corpus rather than by reading:
  a comparison keeps the **vector's** value even when the scalar is the left operand; `sum` and
  `avg` need **Kahan compensated summation**, with `avg` switching from a direct mean to an
  incremental one only once the running sum would overflow; and `or` can map two inputs onto one
  output identity (`-a or -b` both reduce to `{}`), so it must merge them into one column rather
  than emit two.

  Also added: PromQL's **duplicate-labelset error**. Operators that drop `__name__` can collapse
  distinct inputs onto one identity, and returning both silently is wrong.

- **M2b — full-set aggregations.** The operators M2 deferred, all of which need every series at
  a step rather than an incremental fold: `topk`/`bottomk` (per-step bounded heap,
  `O(k × steps)`), `quantile` (`O(series × steps)`, inherent — upstream pays it too),
  `sort`/`sort_desc`/`sort_by_label` (instant-only, so the grid is one step), and the `limitk`
  family. `count_values` is **not** in scope: its schema is data-dependent, and per open
  question 4 it stays unsupported rather than weakening §3.3's invariant.

  Gate: `aggregators.test`, and the `sort` cases in `range_queries.test`.
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
- **M5 — pushdowns.** Port the three existing `storagebackend` pushdowns as planner rules.
  *Gate for replacing the fork on that path.*
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

- **M11 — resource limits.** A `MaxSamples`-style budget (open question 2), counted as the
  engine materializes values and checked as accumulators grow, so a query that would exhaust
  memory fails with a clear error instead of the process dying. Pairs with planner-level
  time-chunking, which bounds the accumulator but cannot bound a genuinely large result.

  No corpus gate — this is about what happens past the corpus' scale.

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
   Failing fast and legibly beats succeeding slowly. **Not yet implemented**; see M11.
3. *(tracked elsewhere)* **Delta-temporality `rate`** — [#1190](https://github.com/oteldb/oteldb/issues/1190).
   Investigating this turned up something larger than the sampling question: oteldb performs no
   delta→cumulative conversion anywhere, so delta-temporality sums reach PromQL as-is, and
   `rate()` over them is already meaningless independent of sampling. The §3.5 matrix therefore
   specifies only the cumulative rule, which is well-defined; the delta branch cannot be written
   until that issue is answered, because it presupposes a semantics that does not exist. This is
   an ingest-and-storage question, not an engine one.
4. *(resolved)* **`count_values`** — **stays unsupported.** It is the only construct whose output
   labels come from observed *values*, so supporting it means either weakening §3.3's invariant
   globally (making `Schema()` able to drain a child, so planning can execute — losing the
   "planning is cheap" property that makes eager resolution safe) or carving out a root-only
   exception. Neither is worth it for one rarely used diagnostic aggregation. It returns
   [ErrUnsupported], which is a visible, honest gap rather than a silent wrong answer, and the
   invariant stays a real invariant.
