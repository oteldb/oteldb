# Storage library integration

Integration work for `go-faster/oteldb` to exploit the `github.com/oteldb/storage`
library's improvements: caches, aggregate pushdown, and lossy precision.

## Cluster observations

`GET /api/v1/storage` on a storage node reports its own ring membership, held compaction claims,
local-known shard replica/EC owner sets, membership fencing and the last enacted rebalance plan.
`placements` covers local engine keys, held claims and last-plan keys; it is not a complete cluster
inventory. `owned` names compaction claims and must not be read as all replica owners. Rebalance
moves describe the last enacted changes, not a predictive dry run.

Per-signal fields expose read gaps, missing/acknowledged-lost parts, index fencing, unestablished
claim tenure and backend space failures. `read_gap_after_unix_nano` preserves the signed boundary;
its minimum int64 value means no parts were known complete. Missing fields on older nodes mean
unknown, while a new node reports explicit false/zero values. These observations distinguish a
node that disclaims incomplete data from proof that the cluster has no data.

The query-only aggregator uses its existing storage fanout to return `node_reports`, including
unreachable nodes. Each entry keeps that node's `engine` snapshot and its own ring/placement
view. New health and placement fields stay out of aggregate totals. Legacy aggregate membership
comes from the first reporting node and `owned` unions reported compaction claims; neither is a
consensus view. `?node=` continues to forward one node's response unchanged.

`private_backend` reports configured peer mirroring. A node-local backend with that option off
is rejected when storage opens, so there is no live `NodeLocalBackendUnshared` flag to report.
These snapshots add no etcd, backend or peer requests beyond the aggregator's existing fanout.

## Metric ingestion

The native backend hashes local gauge/sum pdata points before converting their
attributes. The registry requests a full identity only for a missing series or
overflow routing; known series recover their WAL identity from the registry.
Tenant policy, sampling and admission remain in the storage facade. Resources and
scopes still convert for tenant callbacks. Requests with exemplars, decomposed types
or clustered storage use the existing whole-request conversion path. The pooled
projector releases borrowed input after the synchronous write returns.

`remote_write.enabled: true` enables `/api/v1/write` on oteldb's Prometheus listener when
`metrics_backend: storage` (or `--embedded`) is selected. It uses the existing pooled direct
decoder and native metrics sink, bypassing pdata and the Collector pipeline. It inherits
Prometheus/global authentication and does not apply Collector processors or mirror exporters.
The Collector remote-write receiver remains independently configurable.

```yaml
remote_write:
  enabled: true
  time_threshold: 24h
  max_body_bytes: 64MiB
  max_decoded_bytes: 256MiB
```

The endpoint is disabled by default. Zero limits select the decoder defaults shown above;
negative limits are rejected. Successful writes return 204 after the native sink returns;
write errors retain the decoder's retryable HTTP status behavior. Reused decode buffers stay
borrowed only through the synchronous storage write.

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

Native metric scanners share overlapping series enumeration, aggregates and count grids across
queries. The first overlapping call supplies the snapshot; canceling one caller leaves work running
for the others, and canceling every caller stops it. Admission never waits for a sharing slot.
Streaming raw sample scans and their parallel folds keep their existing ownership and memory bounds.
Metadata flights retain their revision because a later raw scan takes a fresh view; sources without
a revision bypass metadata coalescing. Followers already holding decode quota compute independently.

Completed native results use a 64 MiB cost-bounded Otter cache (`storage.native_query_cache_bytes: 0` disables it), capped
at 4 MiB per entry. Keys include source/backend identity, tenant, canonical matcher fingerprints,
operation and window grid. Local storage revisions invalidate on writes, replay, reset and part publication; age retention
limits reuse to the earliest input timestamp plus the current age policy. There is no freshness TTL.
Clustered or custom sources without a revision capability do not reuse completed results. Custom
sources must explicitly opt into sharing after applying authorization. Setting
`storage.native_query_sharing: false` disables both in-flight sharing and the completed cache.
Raw `rate`/`irate` folds remain uncached. A healthy caller retries a shared context error once with
its own scope. `Open` closes the cache at shutdown; caller-owned `New`/`NewQuery` backends should call
`CloseQueryCache` after serving stops. Runtime cleanup is a fallback for abandoned handles.

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
  subset, including missing-label predicates; hidden internal labels stay residual.
- `promql.MatchesAll(lset, ms)` — the post-fetch full-set re-check (absent label = `""`).
- `promql.PromLabels(series)` — project a `signal.Series` identity (e.g. the one carried on a
  `SeriesAggregate`) to a Prometheus label set, with reserved labels hidden.

So the pushdown path is: `PushableMatchers` → `AggregateMetricsNamed` → `MatchesAll` re-check →
`PromLabels` to render the vector — all using the library's own translation.

Scarecrow's native scanner implements `AggregateScanner` for instant windows and
`GridAggregateScanner` for complete range grids. Both return labeled statistics with exact
`(t-range, t]` bounds. A pinned `@` selector resolves one window and reuses it across steps.
The ordinary native scan remains the fallback for functions the sidecar cannot answer.
`WithOverTimePushdown(false)` disables these capabilities while retaining index counts.
The differential tests compare pushdown results against raw folds and upstream Prometheus.

`rate`/`increase` on native storage still need raw samples for reset detection. ClickHouse retains
its SQL rate-family offloads through `RangeFunctionScanner`; see [the engine contract](promql-engine.md).

## LogQL metric windows

Overlapping log count/rate and byte-count/rate windows consume each input sample and expire it
once, retaining an ordered ring per active group. Integer byte totals avoid subtraction rounding;
noninteger, nonfinite or overflowing sample values retain the floating window fold. Unwrapped
and other aggregations keep their existing evaluators.

Native bucketed sampling adds each record at its first affected step and removes it after its
last. Row presence is independent of byte totals, so empty bodies still produce active zero-valued
groups. Dense difference vectors share a cell budget bounded by the fetched row count; custom
severity groups beyond that budget keep sparse boundary events and visit only active intervals.
Both paths preserve `(step-window, step]` and the existing line/pipeline semantics.

## Loki label predicate pushdown

Unambiguous resource, scope and record attributes use native predicates for every selector
operator, including negative and empty-matching predicates. Missing attributes are evaluated as
empty label values. Normalization collisions and keys observed in multiple scopes stay in the
post-fetch selector so label precedence is preserved.
Record attribute keys matching native column names also stay residual: a storage column condition
would otherwise read the native value instead of the attribute (for example, `body` or `ts`).
Synthetic labels keep their own rules: a missing service name defaults to `unknown_service`,
whereas an explicit OTLP null stays empty; selectors distinguishing them remain residual.

Prepared matchers replace finite whole-label regexps with set membership and literal TraceQL
regexps with substring searches. Other patterns keep their compiled regexp. Pipeline label filters
preserve the engine's own matcher semantics and are pushed only before label-changing stages;
conjuncts may push independently, while disjunctions remain residual.

Finite identity predicates carry `Matcher.StringValues` through fetch and series RPCs. Owners
filter string values but retain missing and other typed values for the exact callback recheck,
preserving pdata's bytes/base64 and composite formatting. These hints never authorize routed limits.

Series, label, and detected-field listings use the same key/scope resolver to pass safe identity
matchers into `LogSeries`, then recheck projected labels. Record and synthetic predicates stay
residual for identity enumeration; they cannot be applied as stream attributes.

## Other touch points

- **TraceQL autocomplete:** filtered tag values reuse candidate selection and the TraceQL evaluator,
  with only the filter and requested-value columns materialized. Resource/scope-only predicates and
  identity values use `TraceSeries` instead, including the exact residual evaluation on each identity.
  Unfiltered suggestions retain dictionary enumeration; kind/status suggestions retain their fixed lists.
- **TraceQL projection:** the engine passes AST-referenced fields through `SelectSpansetsParams`.
  Native fetches retain the identity/timing columns for whole-trace evaluation and root metadata,
  adding attributes, events, links and scalar columns only when required. Attribute names are
  echoed across scopes by the search response, so any attribute reference retains span attributes.
  A nil field list keeps the full-span contract for direct querier callers.
- **Exact predicate hints:** fixed trace-string equality and resolved raw trace-ID sets survive
  native fetch RPCs as `Equal`/`AnyEqual`. Whole-text finite regexps can carry exact sets for raw
  log bodies, trace names/messages, and string/bytes TraceQL attributes; extraction is bounded
  and rejects folding and unsupported forms. Other typed TraceQL attributes never satisfy a
  string regexp. Arbitrary Loki attributes retain exact callbacks because pdata text can differ
  from storage canonical text. Substring alternatives never become equality sets.
  Finite alternatives use VictoriaMetrics' existing extraction after Go-regexp safety checks;
  transient prefilter values use iterators, and only the owned wire byte set is collected and sorted.
- **Legacy Tempo tag search:** native fetch conditions apply intrinsic `name` equality and positive
  span-duration bounds before span materialization. Attribute tags stay with the residual check,
  which preserves span/resource/scope precedence and pdata string formatting. The API's trace
  limit remains after filtering; it is never turned into a span-row limit.
- **Lossy precision:** expose `tenant.Precision{Tiers: []{After, Bits}}` (age-tiered lossy float
  compression) through oteldb's per-tenant resolver, alongside Downsample/Recompress.
  **Implemented** in `internal/storagebackend/policy.go` (`tenancyOption` → `storage.WithTenancy`):
  the `storage.policy` config block exposes `precision[]{after,bits}`,
  `downsample[]{after,interval,agg}`, `recompress{after,level}`, `ec{data,parity,after}`,
  `retention{max_age,max_bytes,signals.<signal>.{max_age,max_bytes}}`
  and `limits{ingest_bytes_per_second,max_in_flight_bytes,max_series,max_series_soft,max_part_size,
  max_merge_part_size}`. `max_part_size` bounds a *flushed* part's uncompressed estimate;
  `max_merge_part_size` bounds a *merged* part's compressed size on disk, and left at zero is
  derived from the backend's free space.
  `storage.policy` is the fallback for tenants without an override. `storage.tenants` maps
  normalized storage tenant IDs to policy overrides; `default` names the default tenant.
  Each override replaces any supplied top-level block (`retention`, `limits`, `downsample`,
  `precision`, `recompress`, `ec`) whole; omitted or null blocks inherit the fallback. Lists
  set to `[]` disable inherited tiers; `retention: {}` and `limits: {}` remove inherited bounds.
  Each resolved policy is validated at startup, with errors naming the tenant.
  A resolver is installed even when only tenant overrides are configured; unlisted tenants
  then retain the unlimited library default. Shard keys resolve through their real tenant
  in the storage library. Policy maps are loaded once at startup, with no hot reload.
  All cluster nodes must share the same configuration: a shard owner decides its policy.

  odbingest already routes writes by tenant. Native query paths still read only `default`
  (tracked in #820); per-tenant retention governs stored non-default data before those tenants
  are queryable. Each signal can set its own age and byte bounds.
  `retention.signals` accepts `metric`, `log`, `trace`, `profile` and `exemplar`.
  A positive signal age replaces `max_age`, longer or shorter; zero inherits the default.
  Signal and pooled byte bounds both apply. A signal byte bound above a positive pooled bound,
  unknown signal names and negative bounds are startup errors. The newest part of each budgeted
  signal remains a storage floor.

  The old `retention.max_bytes_per_signal` key is rejected, including an empty or null value.
  Migrate each entry to `retention.signals.<signal>.max_bytes`:

  ```yaml
  storage:
    reject_out_of_retention: false
    policy:
      retention:
        max_age: 720h
        max_bytes: 100GB
        signals:
          metric: {max_age: 2160h, max_bytes: 30GB}
          log: {max_age: 168h, max_bytes: 40GB}
          trace: {max_age: 72h, max_bytes: 20GB}
          exemplar: {max_age: 72h}
    tenants:
      team-a:
        retention:  # Replaces the entire default retention block; limits and tiers inherit.
          max_age: 2160h
          signals:
            log: {max_age: 720h}
  ```

  Age limits filter facade reads immediately; physical cleanup and byte-budget enforcement are
  eventual. `storage.reject_out_of_retention` optionally refuses expired writes before they mint
  identities/WAL rows; it defaults off to preserve backfill. Rejections appear as
  `out_of_retention` in admission statistics and metrics. Startup logs report effective ages and
  budgets for all signals per overridden tenant (`tenant: "*"` is the fallback), and warn if
  exemplars outlive traces or a metric policy tier starts
  after the metric retention window.

  The storage library's native `Write*` calls return admission counts. The embedded Collector
  sink currently discards those counts and returns success when storage returns no error, even
  when admission sheds some records. Inspect/admission metrics expose these rejections; the
  Collector OTLP response does not. A configured batch processor can acknowledge requests before
  storage admission runs. The native OTLP handler used by `odbingest` reports decoder drops through
  OTLP partial success, but its error-only sink also does not propagate storage admission counts.

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
- **Flush size:** `storage.flush_threshold_bytes` (→ `storage.WithFlushThresholdBytes`) controls
  the per-engine sample-byte trigger independently of `storage.flush_interval`. Zero keeps the
  library default of 64 MiB. Raising the interval alone does not postpone size-triggered flushes.
  Resident identity state, buffer overhead and frozen inputs require additional memory.
- **Disk metric index:** `storage.metric_disk_series: true` keeps historical identities and postings
  in immutable parts and evicts flushed metadata from the resident head. It is disabled by default and requires a file or S3 backend.
  `storage.metric_series_cache_bytes` accepts a positive byte size for each metric engine's
  historical ID/watermark cache; unset uses the library's 512 MiB limit, allocated on demand.
  This cache survives ordinary flushes and is separate from the resident head pressure target.
  `storage.metric_resident_head_bytes` accepts a positive byte size; unset uses the library's
  128 MiB target. One atomic batch may exceed the target, and the next write applies pressure
  maintenance. Large old WALs can require a larger target to replay without changing the journal.
  Upgrade every reader before enabling this mode; existing part identities remain readable.
  Admin signal counts include `series_complete`; a false value means the preserved count is
  unavailable while historical metadata needs repair. Tenant totals expose `total_series_complete`.
  Missing fields identify older nodes that cannot report completeness.
- **Independent metric chunks:** `storage.metric_series_chunks: true` writes independently framed
  per-series timestamp/value chunks and enables encoded concatenation for eligible merges. It is
  disabled by default; existing parts continue through the compatible decode path.
- **Merge memory:** `storage.merge_memory_bytes` (→ `storage.WithMergeMemory`) caps what all
  concurrent merges together hold, and with it the size a merged part reaches before it is sealed.
  It is the write-side counterpart of `decode_memory_bytes`: on a backend that takes objects whole
  a merge buffers its output part encoded in RAM, so free space alone cannot bound it. Unlike the
  caches oteldb adds no default — unset passes 0 through and the library derives a share of
  `GOMEMLIMIT`; negative is unbounded.
- **Query read budget:** `storage.max_query_bytes` (→ `storage.Options.MaxQueryBytes`) caps what a
  single query may hold before it is refused with `readbudget.ErrExceeded`. It closes the gap
  `decode_memory_bytes` leaves: that budget queues concurrent metric queries behind a shared ceiling
  but admits an over-budget query *alone* rather than refusing it, and it never covered the record
  engines at all. Polarity follows the caches, not the library: unset defers to the library default
  (a share of the detected process budget), an explicit `0` disables the bound. The record engines
  reserve an estimate from part metadata before reading and release it when the iterator closes;
  enumeration reads (tag values, series and key lookups) are *not* admitted, because a distinct
  set's size is not predictable up front. `cmd/odbselect` carries its own `max_query_bytes`: an
  aggregator holds every owner's answer at once to merge them, so the owners' limits do not add up
  to one there, and its allowance is what the fan-out declares to each owner.
- **Decode scope:** the record queriers install a `fetch.Scope` and the query's read budget on the
  context (`internal/storagebackend/scope.go`, `Backend.queryContext`) so the reads of one engine
  call are admitted against the decode budget once and bounded by one allowance. The metrics path threads a `Scope` through `fetch.Request` directly. The
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

## Profile timelines

`profilestorage.SeriesQuerier` optionally answers a ProfileQL timeline as one value per half-open
bucket in `[Start, End)`. The native `ProfileQuerier` resolves the profile type and label selector
once, fetches only the value column over the union window, and bins values by timestamp. It does
not load stack ids, symbol sidecars, or flamegraph nodes. Missing legacy values still count as one;
empty buckets return zero, and native reads share the query budget with their output buffer.

The ProfileQL engine parses once and uses this capability when available. Existing queriers retain
the merge-per-bucket fallback. The connect API bounds timelines to 120 buckets, including partial
last buckets; it continues to return one unlabelled total series. Routed readers also need storage
RPC projection forwarding to avoid extra columns from older peers, though totals remain correct.
