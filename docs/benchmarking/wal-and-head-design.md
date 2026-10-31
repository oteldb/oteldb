# Bounded WAL grouping and sealed metric heads

Design proposal, 2026-10-31. Prepared by the requested Astra design subagent and reviewed against the metric ingest/flush paths. Source baseline: [storage 0439604](https://github.com/oteldb/storage/tree/04396046eeba198a11335d578ed8bbb25552a885) (the WAL paths inspected in e9ada72 are unchanged there), and application e99ba24c. The profile used storage 58a975d; proposed changes are not measured implementations. Existing ordering and durability contracts remain requirements.

## Findings and priority

1. The application file backend enables `WithWALDir`, but does not select `WithWALSync` (`internal/storagebackend/open.go`). Storage therefore uses `WALSyncNone` (`options.go`): acknowledgements survive process death through kernel writes, but do not promise power-loss survival until a sync. Rotation and sealing still sync. Preserve that policy, and preserve stronger `Always` semantics for users who select it.
2. WAL writes are already coalesced within each append. `engine/walbatch.go:encode` emits all per-series frames into one reusable `bytes.Buffer`; `flushWALLocked` calls `WriteFrames` once. `wal/segment.go:391` calls one file `Write` and, for Always, one `Sync`. There is no additional `bufio` buffering here. “Fsync per record” in option prose is misleading for this path: one framed batch receives one sync.
3. `engine/engine.go:463` holds the writer gate through admission, WAL encoding, physical write and head publication. The state lock is dropped only for the physical write (`walbatch.go:flushWALLocked`), so readers can run during I/O, but competing writers cannot. Aggregated writer wait proves serialized contention, not that I/O causes it.
4. The repeat's trace shows about 8 seconds aggregate writer wait per 3-second window, but only 114–178ms completed filesystem syscall delay. The current 4096-point append cap already yields roughly 3000 points/write. The existing cap sweep did not establish an end-to-end gain. Cross-request group commit therefore has a conditional benefit, strongest under Always or genuinely small appends; it is not the primary demonstrated fix for this benchmark.
5. The measured large problems are upstream conversion allocation (76.6% of allocated bytes), GC (50.4% CPU), and retained/duplicated head representations. Full flush columns consume exactly 32 bytes/sample before weights: 112.06M detached rows imply 3.34GiB. Grouping WAL writes does not remove these costs.

Recommended sequence: bounded flush construction and memory progress guarantees; native/direct ingestion work independently; WAL stage instrumentation plus small immutable-stage refactor; only then an opt-in bounded ready-group coordinator if measurements justify it. A dense sealed-head representation is the longer-term way to remove the second full-row representation.

## VictoriaMetrics reference: encoded memory parts

The benchmark pins VictoriaMetrics v1.141.0. Its implementation uses a different head shape:

1. [`rawRowsShards`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.141.0/lib/storage/partition.go#L484) distributes append batches among CPU-count shards. Each raw-row buffer is capped at roughly 8 MiB. Pending raw rows are not query-visible; the source sets a 2-second conversion interval.
2. [`rawRowsMarshaler`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.141.0/lib/storage/raw_row.go#L102) sorts each bounded raw-row buffer, groups it into blocks, and writes compressed timestamps, values and index streams into an in-memory part. Conversion is real encoding work, performed before exposing the part to queries.
3. [`inmemoryPart`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.141.0/lib/storage/inmemory_part.go#L16) stores those streams in chunked byte buffers. Queries use the same part reader over them. `MustStoreToDisk` writes those already encoded streams to files and syncs them.
4. [`mergeParts`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.141.0/lib/storage/partition.go#L1427) has a direct-to-disk fast path when finalizing one memory part with deduplication disabled. Combining multiple parts or applying deduplication still takes the merge/encoding path. File I/O and durable publication are still required.

For our engine, the immediate transferable idea is bounded sorting/encoding units. Matching the zero-re-encode promotion also requires building the memory part in the disk representation from the start. Today's detached head is already readable, so giving it a part name does not remove its full-column build. Adopting VM's delayed raw-row visibility would change our read-after-write contract; preserve the current contract instead. The source's default memory-to-disk flush interval is 5 seconds; these paths do not establish our per-ack WAL durability guarantees.

On identity ingestion, VM's [`Storage.AddRows` implementation](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.141.0/lib/storage/storage.go#L1934) first reuses the preceding row's TSID when its complete raw metric identity matches, then checks its metric-name-to-TSID cache. Canonicalization and index lookup/registration are slower miss paths; cache hits still check current partition index membership. This is a cache of the whole label identity, not just the metric name. Our metric projector already computes canonical IDs without per-point identity allocation, and known-series appends skip registration. The measured conversion descriptor/attribute churn occurs before that fast path. Consumer descriptor reuse addresses part of it; a bounded cache or native direct receiver is a separate measured change, not a reason to bypass canonical identity or tenant admission.

## Existing correctness boundary

The engine serializes `admit -> encode -> WAL -> apply`. Admission is state-dependent: per-series newest timestamps, hard/soft cardinality, overflow identity, and active plus detached sample bytes. It cannot safely be parallelized by independently staging requests against the same old head.

`walBatch.admit` maintains a speculative overlay within one write; `apply` publishes only after WAL success. WAL short-write repair restores the pre-write offset before another append. Sync errors differ: bytes may be complete despite an error. A failed acknowledgement is not proof of absence on replay. Grouping must preserve this existing ambiguity and must not pretend to provide exactly-once ingestion.

Flush currently seals the WAL at the same writer barrier as detach (`engine.go:1624–1669`), keeps the detached map queryable, builds outputs, commits the part catalog with a writer-specific covered epoch, swaps query visibility, then checkpoints only the captured sequence. Writes after seal belong to the next epoch. Failure before publication reattaches samples. Ambiguous catalog commits retain the built outputs locally and retain WAL; they must not be retried as fresh independent outputs.

## Increment 1: bound flush output before allocating it

Replace `buildFlushColumns(detached)` followed by `chunkRanges` with a resumable sorted traversal that builds at most one output part's columns at a time. Current `MaxPartBytes` splitting happens after the full allocation and is not a workspace limit.

* Keep one detached generation initially. It remains query-visible for the complete build.
* Build ordered series references once; capture stable identity references, not materialized identities for every point. Bound identity materialization to the current output part, including its encoding workspace.
* Fill a reusable bounded output buffer; encode/write/open each candidate part; release that buffer before the next candidate. Keep only completed part handles/metadata until publication. Preserve row ordering, weights, covered timestamps and part-local identity filtering.
* Publish the complete candidate set and the generation's epoch in one catalog operation, then retire the detached generation and checkpoint its sealed WAL prefix. Do not advance the epoch after writing only some chunks: epoch replay skipping would lose the remainder after a crash.
* On a precommit failure, preserve existing abort/reattach behavior and orphan cleanup. On ambiguous commit, retain the same candidate handles and retry confirmation, as today.
* This bounds column scratch, not all temporary state: sorted IDs scale with series count, and a single unordered series currently invokes `sortedWindow`, which can copy the entire series. Account for those terms explicitly; cap generation rows/bytes next, or implement bounded sorted runs before claiming constant memory for arbitrary out-of-order input.
* Reservation must match the new peak: resident detached inputs are already charged; incremental columns, codecs, identity capture and output metadata must be reserved. Do not keep reserving full-generation flat columns after eliminating them.

The process budget needs a guaranteed flush progress share (or merge admission that leaves enough for one bounded flush unit). Merely giving flush priority in a waiter queue does not help a merge that already owns the whole pool. Reserve the share before starting competing work; yield merge only at safe bounded work boundaries. An unlimited forced acquisition after a long timeout is not a memory bound.

This is the first implementation boundary: metric flush only, same part/WAL formats, same query publication and epoch semantics, no new public asynchronous API. Update `engine/ARCH.md`, memory estimate tests and ADMIN metrics where changed. Do not combine it with group commit or dense heads.

## Increment 2: bounded ready-group WAL coordinator

Use one ordered queue per metric engine, not one global writer and not one goroutine per request. A caller elected as leader may drain the ready queue; a permanent worker is unnecessary initially. Continue accepting the existing synchronous `AppendBatch` contract. A single request must complete before its caller may reuse the borrowed slices and materializer.

Limits must cover request count, input bytes retained while queued, admitted points, encoded frame bytes, and maximum group service time. A request-count-only queue is not bounded memory. A bounded queue does not prevent arbitrary external goroutines retaining already-decoded inputs: integrate its admission with the existing process/tenant ingress admission or reject immediately when full. Count a borrowed request once in process memory accounting, not both as owned and borrowed bytes.

Start with a small cap such as four ready requests / 16384 admitted points, plus an explicit encoded-byte cap; benchmark rather than adopting these numbers as tuning truth. Never wait for a timer in the initial version. Drain only requests already ready and stop at a control barrier. No queue means the current fast path. A zero-delay ready group avoids intentional low-load latency.

### Admission and ordering

Assign a monotonically ordered engine ticket on queue acceptance. Overlapping calls can take either order; completed calls precede newly admitted calls. FIFO order preserves the chosen admission semantics.

The leader acquires writer/state ownership and rechecks close/fence/tenure. Process requests in ticket order using one combined speculative `walBatch`, with a separate `AppendResult` per request. Each request uses its own limits and overflow function; the overlay includes previously admitted requests. This naturally handles two requests introducing the same series or advancing its out-of-order watermark. The staged result does not touch the head.

The existing WAL format groups samples by series. Keep per-series sample order across requests; global cross-series frame order is not a new durable total-order guarantee. Preserve within-request admission order and exact rejection counters. Earlier rejected requests do not acquire a new success just because another request succeeded.

Do not allow an unbounded individual request to defeat the queue cap. Existing projection splits at 4096 points, but the library's public engine API may receive larger inputs. Either retain a documented singleton slow path with its separately bounded ingress reservation, or split at safe request boundaries with explicit partial-success semantics; do not silently invent all-or-nothing request guarantees.

### Identity and buffer ownership

During admission, stabilize all required identities and samples before releasing state. For newly materialized identities, borrowed caller bytes remain valid because the synchronous caller waits; any early-return cancellation requires deep ownership before returning. For existing identities, capture immutable registry views/references under the established identity lifecycle gate. Never move `head.series.GetInto` off-lock merely because encoding looks read-only: it traverses mutable registry state unless a stable view is explicitly captured.

A reusable group batch can own the staged numeric arrays and frame buffer. Its capacity is capped and excess buffers are discarded/trimmed after unusually large requests. Current accumulator capacities survive reset; multiplying that scratch by worker count without bounds would be a regression.

Drop the state lock while retaining writer ownership to encode the stable batch, then write it. Readers see the previous committed head throughout staging/encoding/I/O. Encoding outside state helps readers; it does not parallelize writers or guarantee ingestion throughput gains.

### Commit and acknowledgement

One `WriteFrames` performs one physical write, plus the existing Always sync. All group bytes fit in a single logical write/repair boundary. Existing rotation runs before the group, so the group may exceed the segment target only by the configured group-byte bound; no rotation halfway through a group in the first version.

On WAL success, reacquire state, apply the combined batch, publish the complete group, then complete each waiting caller with its own result. Head publication is the in-process visibility linearization point. The durability point precedes it. A successful caller must be immediately query-visible through the existing query contract.

On write failure, publish none of the group. Report the same write failure to each request with admitted rows, preserve zero-admitted request outcomes separately, and use the WAL's existing healing protocol. On sync failure, publish none, retain the existing may-have-landed failure semantics, and never acknowledge success merely because another request's bytes precede the failing one. For a future API, a typed uncertain-write error could make retry behavior clearer, but that is not prerequisite for the optimization.

### Cancellation and control barriers

The existing engine AppendBatch has no context. Keep its synchronous lifetime in the first version. If a contextual queue is introduced later: cancellation may remove a request before staging; after staging begins, completion waits for WAL outcome and publication. A deadline cannot justify discarding a successful durable write. Returning early requires fully owned data and explicit ambiguous-outcome semantics; do not add that complexity just for batching.

Seal, SyncWAL, checkpoint, reset/reload mutations, fencing and Close must join the same ordering discipline. A seal barrier drains all lower-ticket writes, seals and detaches atomically, then admits later tickets into the new epoch. A checkpoint is only for its captured sequence. It never means “all writes currently queued.”

Avoid a control-path deadlock: a flush must not acquire the writer lock and then wait for a group leader that needs that lock. Queue a barrier before taking writer ownership, or drain the coordinator first using a common control admission gate. Preserve catalog-before-writer lock ordering where required by `engineguard`.

Close marks queue admission closed, wakes/rejects not-yet-accepted requests, drains accepted work to publication/error, then runs final flush/WAL close. A shutdown deadline may leave acknowledged data in WAL; it cannot free staged buffers while a leader still uses them or close WAL underneath a write. Cluster primary/follower quorum acknowledgement remains separate; the initial implementation should keep replication paths unchanged and explicitly prevent local queue bypass from violating ordering.

## Sealed head directly as a part

Yes, a sealed head can become a queryable immutable *memory part*. It cannot become a durable disk part by a pointer swap: today's disk part requires sorted column encoding, identity/index metadata and catalog publication. The existing detached map already gives query visibility without a disk round-trip; renaming it a part saves no allocation by itself.

The useful redesign is a common part-reader interface backed by bounded immutable memory runs and disk parts. Store active numeric samples in generation-owned dense/chunked arenas; use dense local series references inside RAM, preserving canonical 128-bit IDs at WAL/disk/replication boundaries. Sealing transfers ownership of arenas instead of cloning their entire content into `flushColumns`. The disk encoder iterates sealed runs a block at a time.

Identity registry, cardinality and per-series newest timestamps remain engine-wide across generations. A head generation owns samples and pins immutable identity views; it does not reset admission state. Queries atomically acquire a coherent set of disk parts, sealed runs and active-head snapshot. Reference counts or the existing snapshot lifecycle prevent recycling arenas while any query/build still holds them.

Ordering is the main cost: append order is not `(series, timestamp)` order. Options include per-series bounded chunks or sorted row-index runs over immutable arenas. Sorting indices avoids full sample duplication but still costs O(rows) index memory unless runs are bounded. Never sort borrowed live sample arrays in place while queries read them. An ordered-series fast path can encode directly; out-of-order chunks need a stable sorted representation and deterministic tie semantics.

Initially allow one sealed generation plus one active generation, with an enforced byte cap covering both and a guaranteed flush workspace share. If the sealed generation cannot drain, backpressure before active memory grows without bound. Adding many sealed parts is a queue, not a capacity fix. WAL epochs track sealed generations, and only a contiguous durably covered prefix can be checkpointed. A failure leaves the sealed run readable/retryable; a confirmed durable swap removes it atomically. Keeping it sealed rather than reattaching is a later lifecycle change that requires adapting all retry/reload/close paths.

## Measurement and acceptance

Instrument queue depth/bytes, requests and points per group, encoded bytes per group, queue wait, admission CPU, identity capture, encode CPU, physical write time, sync time, apply time, writer hold time and state hold time. Separate original request counts from physical WAL append count. Record seal/barrier latency, retained scratch, active/sealed bytes, flush reservation wait, flush output workspace high-water and completed covered epochs.

Use isolated benches at 1/4/8/16 concurrent writers; 1/128/4096 points per request; existing versus first-seen series; same-series conflicts versus disjoint series; None/Interval/Always; fast fault filesystem versus actual workspace NVMe. Keep end-to-end conversion benchmarks separate from native engine benchmarks. Report points/s, B/op, allocs/op, CPU/point, p50/p99 acknowledgement and read latency, writer/state hold distributions and syncs per request. No throughput claim from aggregate lock-wait seconds alone.

Tests must deterministically block write/sync and inject short writes, repair errors, rotation sync errors, catalog ambiguity and cancellation. Verify competing requests' OOO/cardinality/overflow decisions, identity-before-sample replay, scale factors, per-request rejection results, no publication after failed write, no success before Always sync, query visibility after acknowledgement, readers during encoding/I/O, seal between groups, checkpoint preservation of post-seal samples, Close draining, replica ordering unchanged, and snapshot pinning across retries. Extend existing `walstage_internal_test.go`, `walreaders_internal_test.go`, `wal_durability_test.go` and engine concurrency/faultfs suites; run race tests. Grouping failure tests must include a complete-frame prefix followed by a torn tail.

For bounded flush: row counts and fetched values must match before/during/after flush and restart; workspace growth must stay tied to configured chunk bounds, including weighted/out-of-order and one huge-series cases. Simulate a merge holding its allowed budget and prove one flush can still progress without oversubscription. Retain complete WAL until the complete generation's catalog commit is confirmed.

Accept WAL grouping only if its targeted workload shows a repeatable material gain (predeclare e.g. >=10% throughput or CPU/point improvement), with bounded memory and no material p99 read/ack regression. Retain the zero-queue fast path otherwise. For the live 10k fixture, sustainability requires accepted rate matching offered rate over multiple flush/merge cycles, non-growing queues/WAL/backlog, stable memory, valid fresh query results, and graceful shutdown; a faster append microbenchmark is insufficient.
