# Attribute ingestion: reuse, arenas, and identity caches

Prefer a reusable attribute descriptor/byte arena for the pdata compatibility path.
The isolated prototype eliminates warmed allocation for flat string attributes in
both repeat and churn workloads. A canonical hash dictionary wins slightly on hits,
but loses substantially under churn and retains another copy of identity state.
This is a scoped conversion result, not a native 10k capacity claim.

Research baseline: app `ef4be27119cfbb452c1c83b15b9073be2f5978ba`, storage `v0.50.0`,
Go `go1.27.3-X:nodwarf5`, Linux/amd64, Ryzen 9 9950X3D2, GOMAXPROCS=8.
No production implementation, API, or persisted format changes are included.

## Where the copies happen

`internal/storagebackend/storagebackend.go:ConsumeMetrics` already borrows native
metric descriptors across calls. Storage `otlp/pdataconv/pdataconv.go:convertMap`
still allocates a KeyValue slice, every key, every string value, and nested values.
`convertValue` calls pdata Bytes.AsRaw, which also copies. Resource/scope schema URLs,
scope name/version, metric name/unit and exemplar IDs require the same accounting.
`signal/metric/metrics.go:AddPoint` explicitly discards its previous Attributes;
simply passing the old attributes to convertMap cannot reuse them without a builder
change or separately owned arena.

On this amd64 build, `signal.KeyValue` is 80 bytes and `signal.Value` is 56 bytes.
Four flat string attributes therefore need 320 descriptor bytes plus payloads.
The fixture copies 54 payload bytes/map, but independent small allocations and
rounding cost 88 B/map: baseline is 408 B and nine allocations/map. Reusing only
KeyValue storage removes 320 B and one allocation, leaving eight payload allocations.
An arena still copies those 54 bytes; it removes allocation granularity and GC objects.

The live results in [metrics-consumer-reuse.md](metrics-consumer-reuse.md) identify
RW-to-pdata as a separate large cost. This proposal cannot remove that conversion.
The existing `internal/promrw.Converter` already uses descriptor arenas and aliases
request bytes; `cmd/odbingest` uses it. Routing `cmd/oteldb` through that path is a
separate compatibility/wiring decision, with potentially greater benefit. Its prior
conversion microbenchmark excludes decode, WAL and the full server pipeline.

## What hashing already does

Storage `signal/metric/metric.go:Project` already hoists resource/scope and metric
identity prefixes and merges sorted point attributes into reusable hash scratch.
`signal.HashBytes` is a canonical 128-bit XXH3 identity, not a new optimization.
`index/series/compact.go` already stores dense local references, shared contexts,
keysets and typed symbol IDs. Replacing that with another string-to-ID map duplicates
existing machinery. A cache after pdataconv cannot avoid pdataconv's copies.

A useful cross-batch cache must run before native descriptor construction. It must
read pdata, sort keys, build typed canonical bytes, probe, verify candidate equality,
and only construct/own descriptors on a miss. Hits avoid descriptor construction,
but still inspect and encode incoming labels. A label-set cache does not produce a
complete series ID: resource and scope schemas/names/versions/attributes and the
five reserved metric fields (name, unit, kind, temporality, monotonic) also identify
a series. Tenant separation must be preserved by cache scope or key even where the
canonical series hash itself excludes tenant. Samples and exemplar identities must
not be folded into the parent series identity.

Canonicalization must preserve type tags, byte lengths, nested map key order,
ordered arrays, floating-point bit patterns (including signed zero/NaNs), and the
current stable duplicate-key behavior. The native RW converter rejects duplicate
labels; the generic attribute constructor assumes uniqueness but sorts stably.
Do not replace this with concatenated text labels or a 64-bit fingerprint. Existing
series index dedup trusts the canonical 128-bit ID; the prototype explicitly verifies
canonical bytes before a cache hit and tests forced collisions.

## Prototype and measurements

[attributeprototype/attributes_test.go](attributeprototype/attributes_test.go) contains
only test code. One operation converts 1,024 pdata maps, each containing `instance`,
`job`, `mode`, `cpu`. Construction is outside timing. Maps arrive in unsorted order.
Repeat cycles 1,024 identities; churn cycles 4,096 identities against a 2,048-entry
dictionary. All variants receive an initial 1,024-map warmup. Churn therefore includes
one initial hit batch, then repeated misses/whole-cache eviction. It is not a cold-start
measurement. The dictionary's own map growth, candidate serialization, collision
comparison, miss copies and eviction are included in timed work.

Six 200ms samples, medians from benchstat; bytes/objects are per 1,024-map operation:

| Path | Repeat time | Repeat B/op | Repeat allocs/op | Churn time | Churn B/op | Churn allocs/op |
|---|---:|---:|---:|---:|---:|---:|
| Copy descriptors and bytes | 160.1 µs | 417,793 | 9,216 | 183.0 µs | 417,793 | 9,216 |
| Reuse descriptors, copy payloads separately | 130.4 µs | 90,112 | 8,192 | 136.9 µs | 90,112 | 8,192 |
| Reuse descriptors and byte arena | 80.25 µs | 0 | 0 | 80.10 µs | 0 | 0 |
| Canonical hash dictionary | 73.25 µs | 0 | 0 | 299.8 µs | 499,457 | 10,227 |

The dictionary hit advantage over the arena is about 9%; its churn time is about
3.7 times the arena's. These are observations from one focused experiment, not
end-to-end speedups. The baseline's repeat time varies by 15%; other timing spreads
are 0–2%. Raw samples are in `attributeprototype/results.txt`.

After 1,024 maps the arena's latest backing buffers total 475,104 bytes: 57,344 byte
capacity and 5,222 KeyValue slots. At stable size those two backing allocations can
serve subsequent batches. Cold growth allocates additional arrays and may retain
previous byte slabs through descriptors until reset; this is not zero-allocation
cold ingestion. The dictionary owns 499,712 bytes of payload/descriptor capacities
for 1,024 entries, excluding map storage, 48-byte entry values, scratch and allocator
rounding. This lower bound is already larger than the arena's latest buffers and
scales with cache capacity rather than only in-flight batch size. Neither figure is
a whole-process retained-heap measurement.

The prototype supports only string labels and panics for other pdata types. It does
not implement full series identity, byte-budget eviction, synchronization or lifecycle
integration. The cache uses whole-map clearing to expose the miss tradeoff; a better
replacement policy needs its own measurements. An entry limit alone cannot bound memory
with arbitrary value sizes. Inputs and dictionary fixtures remain alive throughout;
no claim about steady RSS or GC percentage follows from this microbenchmark.

Tests pass for reordered keys, source mutation after conversion, arena growth,
outstanding cache results across eviction, forced collisions, and canonical preimage
parity. Fuzz corpus includes embedded zero bytes and repeated keys (pdata resolves
the latter before conversion). Only corpus cases were run; no extended fuzz campaign.

```
GOMAXPROCS=8 go test -p 4 ./docs/benchmarking/attributeprototype -v
GOMAXPROCS=8 go test -p 4 ./docs/benchmarking/attributeprototype \
  -bench BenchmarkAttributeConversion -benchmem -benchtime=200ms -count=6
benchstat docs/benchmarking/attributeprototype/results.txt
```

## Concrete next patch: owned converter arena

Keep the existing exported `pdataconv.AppendMetrics` semantics unchanged. Add an
opt-in `MetricsConverter` in storage `otlp/pdataconv/metrics_converter.go`, with
`AppendMetrics(dst *metric.Metrics, md pmetric.Metrics) Dropped` and `Reset()`.
The zero value is usable, not concurrency-safe. Append borrows the converter's
owned storage into dst; every result remains valid until Reset. Reset is permitted
only after the complete synchronous WriteMetrics return, including deferred exemplar
handling. App `ConsumeMetrics` should pool converter and native batch together so a
converter can never be reset while another request uses its bytes.

The first implementation handles gauges/sums, including arbitrary typed attributes
and their exemplars. Existing helpers handle decomposed histograms/summaries unchanged;
mixed batches must preserve original order, dropped counts and fallback ownership.
No attribute type is silently dropped by the fast path. Refactor private helpers
inside pdataconv rather than duplicating histogram semantics in the application.

Use separately owned arenas for KeyValue, Value and bytes. Copy strings with append;
copy pdata byte values through ByteSlice.At into reserved arena space to avoid the
intermediate AsRaw allocation. Inline scalars preserve bits. Recursively allocate
nested map/array descriptors, sort only the newly allocated map segment, and use
full slice expressions to prevent later appends from overwriting neighbors. Reserve
a parent's complete descriptor segment before recursion and re-index after any
arena growth; do not retain a pointer into a slice that recursion may reallocate.
Boxing nested slices into Value.ref may still allocate; measure this separately.
Copy resource/scope schema/name/version, metric name/unit, exemplar filtered attrs,
TraceID and SpanID through the same ownership machinery.

A segmented arena or exact per-batch pre-count can avoid repeated growth copies;
start with the simpler measured implementation. Pre-counting doubles traversal,
so compare it on real labels/nested values before adopting. Clear used descriptor
slots on reset, clear all previously active nested batch references (including tails
left by large-to-small requests), then rewind byte storage. Limit retained capacity
in bytes, including descriptor sizes, with oversized converters discarded on return.
Deep nesting and huge cells may need dedicated allocations dropped after the request;
zero objects for every AnyValue kind is not a promise. The pool is a reuse hint, not the limit; in-flight request admission and concurrent
converter count remain separate memory controls. Do not choose a universal size cap
from this 1,024-map fixture.

Before enabling payload reuse, patch storage `engine/walbatch.go:reset`: clear each
active accumulator's borrowed `series` and clear/truncate its `attrs` workspace while
retaining numeric timestamp/value/sampling buffers. Do this before truncating order.
The current code leaves shallow series fields in accs. Current encode/apply iterate
order, and add overwrites an accumulator before making it active, so inactive stale
fields are not evidence of an active use-after-return today. Clearing them makes the
ownership boundary explicit and stops retaining borrowed buffers. Include successful,
failed encode/WAL, cancellation and retry resets; no reset may precede encode/apply.
`metricAppendBuffer.reset` already clears its borrowed sources. Audit cluster frame
ownership and deferred exemplars in the same test change.

Required tests before app opt-in:

- Table tests for every scalar, empty, bytes, nested array/map; canonical preimage
  equality with existing conversion, including floats and reordered nested maps.
- Fuzzer seeded by those tables; bounded depth/size, source mutation and repeated reset.
- Two batches with changed bytes, metadata, schemas, resources, scopes and exemplars;
  mutate source and overwrite arenas after each WriteMetrics, then query before flush,
  after flush and after reopen. Include mixed histogram/summary fallbacks.
- Large-to-small-to-empty reset checks inspect every retained tail and use deterministic
  ownership assertions. Include nested arena growth and unrelated concurrent converters.
- WAL failure/retry, cancellation and cluster replication framing; race tests with
  ingestion/query/flush. Prove exemplar payloads remain correct after return and reuse.
- Full adapter benchmark including WriteMetrics, plus cold start, oversize requests,
  deep typed values and churn. Only then repeat the bounded live load with allocation,
  object, RSS, accepted/offered and queue-growth measurements.

## Further compactness work

Encoding directly from `index/series.View` is a distinct storage-side opportunity.
`View.At` decodes interned typed values into Attributes; `identity.EncodeSorted` then
encodes/interns them again into a part-local symbol table. Expose an immutable encoded
view or visitor that copies existing typed symbol bytes and remaps old local symbol
IDs to part-local IDs. Preserve the existing disk format and resource/scope semantics;
never persist live index ordinals. This can remove nested decode allocations and
re-encoding without changing identity. Benchmark flush CPU and peak workspace against
real high-cardinality data, since flat values and reused attribute scratch already
avoid much allocation. It will not remove pdata conversion.

An ID-only ingest API or cross-request dictionary is a later architectural choice.
It requires cache byte budgets, cold misses, tenant isolation, registration ownership,
pruning/generation fencing, restart/recovery and a canonical identity fallback for WAL
and replication. Reusing a dense reference after prune or restart can target another
series. Keep canonical 128-bit identity at durable/transport boundaries; scope local
refs to a specific registry generation. The measured arena path provides a useful
first improvement without introducing those lifecycle requirements.

## Hash before conversion, using the existing registry

There is a stronger alternative to the second attribute dictionary measured above:
compute the complete canonical SeriesID directly from incoming pdata using reusable
scratch, then let the engine's existing series registry decide whether to materialize
native attributes. The unfavorable dictionary churn result does **not** reject this
approach. It has no second cross-batch identity cache. The arena remains the measured
improvement available behind the current facade; this lazy source is an unmeasured,
potentially larger improvement requiring a new internal ingest seam.

The relevant storage v0.50.0 code already supports the engine half:

| Source location | Existing behavior relevant to lazy conversion |
|---|---|
| `engine/engine.go:463`, `Engine.AppendBatch` | Accepts ID/timestamp/value columns and a per-index materializer, under one writer lock. |
| `engine/walbatch.go:62`, `walBatch.admit` | Buffered identities need no materializer; registered identities starting a new buffer need no materializer; new admitted identities and soft-overflow transformation do. Hard-limit rejection can avoid it. |
| `engine/walbatch.go:184`, `walBatch.encode` | Reconstructs a known identity from `h.series.GetInto` when it must relog the identity for a new buffer. It does not require the producer to resend native descriptors. |
| `metric_append.go:16`, `metricAppendBuffer` | Already batches IDs and point references, but captures concrete `metric.Source` values. |
| `signal/metric/source.go:6`, `Source` | Hardcodes native resource/scope/Metric pointers; cannot describe pdata-backed points. |
| `metric_write.go:33`, `metricWriter.write` | Resolves tenant, enforces policy, batches and samples before engine append. |
| `exemplars.go:31`, `writeExemplars` | Reprojects the native Metrics input and writes a separate record engine after successful metric ingestion. |
| `cluster/framemetrics.go:59`, `FrameMetrics` | Materializes and sends each distinct admitted series once per shard in each request. |

A local repeat-series path can therefore be:

```
pdata/request
  -> typed canonical hash scratch + sample columns + stable source coordinates
  -> tenant/policy/admission
  -> Engine.AppendBatch(ids, timestamps, values, weights, materialize)
       known ID -> append sample (WAL identity relog from existing registry)
       new ID   -> materialize this identity -> own/register -> append sample
```

Hashing still reads every attribute and copies canonical bytes into scratch. What it
avoids on a hit is the second, native KeyValue/Value/payload object graph, including
its sorting and copying. Sort reusable references to pdata entries rather than
allocating native KeyValues; encode strings directly, scalar bits with the existing
canonical type tags, and recursively sort map entries with bounded scratch. Hoist
resource/scope and metric prefixes as the current projector does. Keep point fields
and reserved metric labels merged with exactly the current precedence. A production
encoder must match `Identity.ToSeries().AppendHashInput` byte-for-byte, not merely
produce equal IDs on the common string-only fixture.

Cold/churn paths still hash every point and then convert new identities, but they
avoid constructing a duplicate dictionary and retained cache entries. They may pay
additional traversal or new-series work under the engine lock; neither superiority
over the arena nor acceptable contention is established without measurement. The
current measured dictionary uses canonical scratch too, but its hit/miss costs cannot
be transplanted: it hashes only point string labels, maintains its own map, and has
no storage admission, lock, WAL or lifetime integration.

### Optional prepared source seam

A pdata-free prepared source contract could live beside `signal/metric` and be
consumed by the storage facade, with its adapter implemented only in the optional
`otlp/pdataconv` package. The contract would expose bounded sample-column batches,
resource/scope context for existing tenant callbacks, and a stable source plus point
index for lazy Series materialization. A native adapter would continue to serve
current callers. This is a contract sketch, not a proposed public API signature.

Keep IDs computed freshly from this request. The trusted adapter owns correctness
between ID and materializer; do not expose unchecked caller-supplied IDs as a network
protocol. Existing registry hit handling trusts the 128-bit ID, so this path preserves
that collision model rather than adding candidate comparison or a new fingerprint.
There is no retained dense reference to become stale: after prune/reset/reopen, an ID
miss simply materializes from the still-live current request. Registration and WAL
ordering remain the engine's existing authority. This is different from a producer
sending an opaque cached local ID and needing dictionary registration generations.

Integration requirements are substantive:

- Generalize the concrete Source capture in `metricAppendBuffer` and the materializer
  used by `metricWriter.appendSampled`. Sampling filters must retain original source
  indices. A callback must not close over a reused batch header or hash scratch.
  Preserve metric grouping/order: changing batch boundaries can change whole-batch
  rate shedding and sampling behavior even when the samples are identical.
- Preserve `Storage.WriteMetrics` closed/read-only checks, tenant resolution, reserved
  tenant rejection, sampling weights, cardinality/in-flight limits, soft overflow,
  counters and partial-success accounting. Calling Engine directly from an app adapter
  bypasses these rules. Tenant callbacks accept full typed Resource/Scope and may
  inspect arbitrary attributes; materializing those once per group is a compatible
  compromise. A zero-native-descriptor claim for all context fields is unjustified.
- Exemplars need a prepared-source projection or an explicit compatibility fallback.
  Their independent registry may need a full identity even when the metric registry
  already knows it. Preserve current best-effort/drop counting and the deferred
  write-on-success behavior; do not silently switch to per-point accepted-only
  exemplar semantics. A first local gauge/sum fast path can fall back for the entire
  request when exemplars or decomposed types occur, avoiding mixed-order surprises.
- Cluster framing cannot assume a remote primary knows an ID. Current frames carry
  each identity once per shard per request, even if it was previously ingested. A
  prepared source can supply those identities on demand, but local registry hits do
  not eliminate their conversion/encoding at the origin. Preserve existing frames,
  shard routing and origin admission; use the native fallback initially. Omitting
  identities from the wire would be a separate protocol/recovery design.
- Validate types and structural limits before admission: the current materializer
  has no error return. New-series conversion failures must not panic inside an engine
  lock or leave partial registrations. Existing gauge/sum value-less point and
  exemplar drop behavior, and integer-to-float sample conversion, stay unchanged.

The request, its read-only pdata objects, and captured source coordinates must remain
valid until the entire facade write returns, including final buffered append and
exemplar processing. Materialized misses must remain stable for the whole engine
append: WAL staging stores shallow Series values until encode/apply, so reusing a
single callback scratch identity for several misses would corrupt earlier entries.
Use per-call arenas for all materialized misses, released only after those consumers
finish. Never mutate pdata during hashing/materialization; callbacks must not call
back into the engine or acquire locks that invert its writer lock. Retained callback
sources must be cleared on every success/error/reset path. The earlier WAL reset and
bounded arena ownership requirements still apply.

### Validation before selecting the lazy path

Property tests should compare complete canonical preimages and SeriesIDs against
existing pdata conversion plus native projection for all AnyValue kinds, nested map
orders, duplicate policy, metadata/context changes and reserved-label collisions.
Also assert that each materialized identity hashes to its advertised ID. Include
sample ordering, missing values, decomposed fallbacks and tenant separation.

Instrument materializer calls in engine/facade tests: repeated known series should
invoke none on the no-exemplar local path; first registration should invoke only as
needed; a known series after head flush must relog/recover correctly without calling
the producer materializer. Exercise multiple new identities in one WAL append,
prune/reset/reopen misses, concurrent ingestion, soft overflow, all limit rejections,
WAL errors and retry. Assert registry/query/WAL contents after source mutation and
arena reuse, and run race tests. Add compatibility cases for tenant callbacks,
sampling index remapping, deferred exemplars and cluster fallback.

Then compare three complete paths: current adapter, owned arena adapter, and prepared
hash/lazy adapter. Measure cold registration, warm repeats, churn, nested values,
small/large batches, tenant switches, exemplars and concurrent writers. Count hash
scratch allocations, source-coordinate storage, materialization calls, registry bytes,
WAL relog materialization, lock contention, total objects and retained memory. Only a
subsequent bounded live run can establish a throughput/GC benefit. No extra prototype
or benchmark was run for this design extension while other agents held measurement
slots.
