# Storage library integration

Integration work for `go-faster/oteldb` to exploit the `github.com/oteldb/storage`
library's improvements: caches, aggregate pushdown, and lossy precision.

## Caches / optimizations are opt-out for oteldb

The storage library keeps these opt-in (it's a general-purpose library whose default is the
in-memory test backend):

- `Options.ReadCacheBytes` — `0` = off
- `Options.DecodeCacheBytes` — `0` = off
- `Options.AggregateStats` — `false` = off

**oteldb must flip the polarity: enable all three by default**, sized from available RAM,
exposing flags/env to disable them (e.g. `--storage.read-cache-bytes=0`).

Optionally, add a `storage.RecommendedOptions(backend, ramBytes)` helper to the library so the
sane-sizing logic lives there and oteldb just opts into the bundle.

> **Implemented** in `cmd/oteldb/storage_backend.go` (`resolveCacheSettings` / `cacheOptions`):
> all three default on. `read_cache_bytes` / `decode_cache_bytes` size to a fraction of the Go
> memory limit (floors 128 MiB / 64 MiB); an explicit `0` disables a byte cache and
> `aggregate_stats: false` disables the sidecar. Config block: `storage.{read_cache_bytes,
> decode_cache_bytes,aggregate_stats}`.

## Aggregate pushdown for `*_over_time` (headline integration)

Call the facade instead of raw-fetch-and-fold:

- `Storage.AggregateMetricsStep(ctx, tenant, req, stepNs)` — range vectors (per-step buckets)
- `Storage.AggregateMetrics(ctx, tenant, req)` — whole range; `map[SeriesID]SeriesAgg` (unlabeled)

Returns `engine.BucketAgg{Start, SeriesAgg{Count, Sum, Min, Max}}` per series.

**Labeled variant (use this for rendering):** `Storage.AggregateMetricsNamed(ctx, tenant, req)`
returns `[]storage.SeriesAggregate`, each pairing a `signal.Series` identity with its whole-range
`engine.SeriesAgg`. This is what oteldb wants for the `*_over_time` path: the identity rides along
from the same sidecar pass, so oteldb renders the result as a PromQL vector (labels + value)
**without a second, value-decoding fetch**. Use the unlabeled `AggregateMetrics` only when the
aggregate alone is needed. Series with no sample in the window are omitted. Cluster fan-out is
labeled-aware (`clusterAggregateNamedFor` re-checks the full matcher set per shard and unions).

**Covered:** `count`, `sum`, `min`, `max`, `avg`, `present_over_time`.

**Not covered:**
- `rate` / `increase` — need per-bucket first+last value + counter-reset count. A richer sidecar
  is the future enhancement; high value since `rate` is the most common function.
- `last_over_time` / `first_over_time` / `quantile_over_time`.

The Prometheus engine has **no `*_over_time` pushdown hook**, so oteldb needs a pushdown-aware
eval path that recognises aggregation-over-time and delegates to the facade; other functions fall
back to the existing `Queryable` (raw fetch).

**Reuse the adapter's Prom↔storage translation — do not duplicate it.** The `query/promql` package
now exports the projection helpers the `Queryable` uses, which are the single source of truth for
the seam oteldb's pushdown path sits on:

- `promql.PushableMatchers(ms)` — lower a Prometheus matcher set to the index-safe `fetch.Matcher`
  subset (matchers that match `""` are not pushed; they stay for the post-fetch re-check).
- `promql.MatchesAll(lset, ms)` — the post-fetch full-set re-check (absent label = `""`).
- `promql.PromLabels(series)` — project a `signal.Series` identity (e.g. the one carried on a
  `SeriesAggregate`) to a Prometheus label set, with reserved labels hidden.

So the pushdown path is: `PushableMatchers` → `AggregateMetricsNamed` → `MatchesAll` re-check →
`PromLabels` to render the vector — all using the library's own translation.

> **Implemented** for both **instant** and **range** queries, wired through
> `scanners.NewMatrixSelector`:
> - Instant: `aggregateOverTimeOp` (`internal/storagebackend/overtime.go`) folds one
>   `AggregateMetricsNamed` over the single eval window.
> - Range: `aggregateOverTimeRangeOp` (`internal/storagebackend/overtime_range.go`) folds one
>   aggregate per `(series, step)` over each step's exact sliding window `(t-range, t]` — the same
>   window the matrix selector uses — and streams the per-step vectors, so it holds `O(result)`
>   instead of materializing every raw sample in every window. The storage engine still applies its
>   own sidecar/decode fast paths (and decode cache) per window; folding one aggregate per part per
>   step in a single decode pass (rather than re-decoding overlapping windows) is a follow-up.
>
> Both paths fall back to the raw matrix selector for selectors carrying a projection, per-series
> filter, or `@` modifier, and for folds the sidecar cannot answer. Covered folds:
> `count`/`sum`/`min`/`max`/`avg`/`present_over_time` (`overTimeFold`). Toggle with
> `WithOverTimePushdown` (on by default). Correctness is pinned by the differential oracle
> (`TestOverTimePushdownRangeMatchesRaw`): pushdown-on vs the raw fold must produce an identical
> matrix across folds, matchers, offsets, and window/step combinations.
>
> `rate`/`increase` still need a richer sidecar (per-bucket first+last value + counter-reset count)
> and remain on the matrix selector — tracked as the #1117 follow-up.

## Other touch points

- **Lossy precision:** expose `tenant.Precision{Tiers: []{After, Bits}}` (age-tiered lossy float
  compression) through oteldb's per-tenant resolver, alongside Downsample/Recompress.
  **Implemented** in `internal/storagebackend/policy.go` (`tenancyOption` → `storage.WithTenancy`):
  the `storage.policy` config block exposes `precision[]{after,bits}`,
  `downsample[]{after,interval,agg}`, `recompress{after,level}`, `ec{data,parity,after}`,
  `retention{max_age,max_bytes}`
  and `limits{ingest_bytes_per_second,max_in_flight_bytes,max_series,max_series_soft,max_part_size,
  max_merge_part_size}`. `max_part_size` bounds a *flushed* part's uncompressed estimate;
  `max_merge_part_size` bounds a *merged* part's compressed size on disk, and left at zero is
  derived from the backend's free space.
  oteldb runs the embedded engine single-tenant, so a static `tenant.ResolverFunc` returns one
  policy for every tenant — retention is therefore one global window, not per-tenant.
  Both `retention.max_age` and `retention.max_bytes` are enforced by the library.
  `ec` is an age tier like `recompress`, but for durability: cold parts are stored as `data`+`parity`
  Reed-Solomon shards, one per cluster node, instead of RF full copies. It applies only under
  `storage.cluster` with `private_backend` — on a shared object store the store owns durability and
  the policy is inert (logged as a warning at startup). Owner count is exactly `data+parity`; the
  replication factor is ignored under EC.
- **Sampling weights:** honour `fetch.Batch.ScaleFactors` in PromQL `sum`/`rate`/`count` for
  sampled tenants. The aggregate sidecar is skipped for sampled parts, so those fall back to a
  weighted raw fold. **Not implemented.** oteldb does not yet expose a `tenant.Sampling` policy,
  so ingest is always lossless and every `ScaleFactor` is `1` — honouring weights is a no-op
  until sampling is configured. It is also not a pure sample-level transform (the weight folds
  differently for `count` vs `sum` vs `rate`), so the correct home is either a weight-aware fold
  in the library `query/promql` queryable or a dedicated pushdown — a design decision deferred
  with sampling itself.
- **Merge memory:** `storage.merge_memory_bytes` (→ `storage.WithMergeMemory`) caps what all
  concurrent merges together hold, and with it the size a merged part reaches before it is sealed.
  It is the write-side counterpart of `decode_memory_bytes`: on a backend that takes objects whole
  a merge buffers its output part encoded in RAM, so free space alone cannot bound it. Unlike the
  caches oteldb adds no default — unset passes 0 through and the library derives a share of
  `GOMEMLIMIT`; negative is unbounded.
- **Decode scope:** the record queriers install a `fetch.Scope` on the context
  (`internal/storagebackend/scope.go`) so the reads of one engine call are admitted against the
  decode budget once. The metrics path threads a `Scope` through `fetch.Request` directly. The
  boundary is one engine call, not one HTTP request: the LogQL/TraceQL engines own the per-request
  boundary above the querier, so a query evaluating several pipeline nodes still opens a scope per
  node.
- **Merge visibility:** `SignalStats` reports `SealedParts`, `MergeBacklog` (now parts − sealed,
  not the part count), `MergeCandidates` and `MergeCapBytes`; the admin API surfaces all four and
  the Storage page flags backlog-with-no-candidates as *stuck*. The `storage-compact` action calls
  `Admin.CompactNow`, which overrides only the selection heuristic — the seal threshold and merge
  memory bound still apply — and is the escape from that fixed point.
- **Stream costs:** `Storage.StreamCosts` attributes a record signal's parts to streams, or to a
  stream label's values, with per-column distinct estimates. Exposed as
  `GET /api/v1/storage/stream-costs` and the admin UI's Stream costs page. It is the heaviest call
  the library exposes — every accounted byte column of every live part is decoded once — so the
  page runs it only on an explicit Analyze, never on mount, focus or interval, and the endpoint
  bounds an unset `top_n` to 20. The signal parameter is a `RecordSignal` (traces/logs/profiles):
  metrics carry no per-record columns, so the library's rejection of them is unrepresentable here.
- **Cluster:** aggregate fan-out is automatic — just call the facade per tenant.
- **Metadata:** querier `LabelValues` / `LabelNames` are implemented in the promql `Queryable`
  adapter. Since oteldb/storage#262 they answer from the postings index rather than draining
  samples; oteldb gets that for free, because the capability is detected on the `fetch.Fetcher`
  it already passes in.
