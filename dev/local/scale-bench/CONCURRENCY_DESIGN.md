# Ingestion, maintenance, and snapshot concurrency

Status: design proposal, 2026-10-27. No concurrency changes or performance claims are implied.
The first implementation is bounded storage-boundary batching across metrics, preserving
input order, WAL-before-apply, and the existing policy/rate/sampling path where necessary.

Storage source inspected: sibling worktree `../compact-series-storage`, branch
`claude/compact-series-index`, base `9765fda843d16b52d8bd40ff854e8e9412e11cc6`, including its
uncommitted compact identity/index changes. Source line references describe that inspected tree.
`PROMPT.md`, `DESIGN.md`, and `_ref` were absent from the worktree and original storage checkout;
this proposal uses committed architecture, implementation, and the primary references below.

## Evidence and immediate priority

[PR #1441](https://github.com/oteldb/oteldb/pull/1441) records the diagnostic 10k-host, 15s-scrape fixture:
approximately 967k offered versus 250k accepted points/s, about one WAL append per accepted
point, and 28.57 aggregate writer mutex-wait seconds in a five-second runtime trace.
The trace measures waiter delay; it does not attribute all contention to maintenance.
Batch-shape microbenchmarks do not predict full-cardinality throughput.

The next steps are batching, shorter identity-capture critical sections, publication outside
the ingestion mutex, and overlapping bounded flush/merge builds. Parallel appenders and
multiple concurrent flush generations come only after measurement justifies their complexity.

## Current ordering and remaining work

Paths below are relative to the inspected storage worktree.

| Implementation | Existing guarantee or cost |
|---|---|
| `engine/engine.go:455` | `AppendBatch` admits, logs, and applies under `Engine.mu`. |
| `engine/engine.go:1599`, `engine/merge.go:110` | Flush and merge hold the same `flushMu` throughout their bodies. |
| `storage.go:1459` | The facade calls flush followed by merge for an engine. |
| `index/series/view.go:9` | `series.View` captures append-only arenas without materializing identities. |
| `engine/partidentity.go:79` | Flush still constructs its identity-reference map under the engine lock. |
| `engine/streampart.go:264` | Streamed merge resolves all output identities under one read lock. |
| `engine/index.go:59` | Manifest CAS, conflict reload, and adopted-part opening execute from a locked commit path. |
| `engine/head.go:168` | Pruning rebuilds the registry, changing dense identity references. |
| `wal/segment.go:27` | `SegmentWriter` is explicitly not safe for concurrent use. |

`engine/ARCH.md:9` says the engine lock never spans object-store I/O, but the locked index
commit path does. Correct that statement when implementing the new publication protocol.

## Required event ordering

```text
project bounded batch
  -> ordered admission -> successful WAL write -> apply active head -> acknowledge
  -> WAL seal + head detach at one writer barrier
  -> frozen generation remains queryable
  -> build all output objects, identities, and sidecars off-lock
  -> CAS durable part set + this writer's covered epoch
  -> atomically replace frozen generation with output parts in the read view
  -> checkpoint only the captured sealed WAL prefix

reserve and pin merge inputs
  -> build outputs off-lock
  -> validate ownership, input lineage, and rollup compatibility
  -> CAS replacement against the latest durable catalog
  -> atomically replace inputs in the read view
  -> retire inputs -> reclaim after readers and builders release them
```

Publication applies an operation delta to the latest catalog: add flush outputs, or replace
these exact merge inputs. Installing the entire old planning snapshot can lose newer parts.

## Gates and lock hierarchy

| Mechanism | Responsibility | Excluded work |
|---|---|---|
| Writer gate, e.g. `writeMu` | Admission, WAL operations, apply, seal/detach, conflicting head mutation | Part building and manifest CAS |
| `Engine.mu` | Short head changes, coherent read-view installation, references | WAL/object-store I/O and identity encoding |
| Publication coordinator, e.g. `publishMu` | CAS/rebase, reload/install, reset, prune installation, ownership transitions | Whole flush/merge/prune builds |
| Bounded build permits | CPU, scratch memory, and output buffering | Waiting while holding `Engine.mu` |
| Snapshot references | Lifetime of frozen buffers, identity arenas, and parts | Early pooling, reuse, or deletion |

When nested, acquire `publishMu -> writeMu -> Engine.mu`. Ordinary append takes only
`writeMu -> Engine.mu`. Never wait for an outer gate or a build permit under `Engine.mu`.
Shutdown closes admission, drains owned work, then closes the WAL and releases snapshots.
Reset must invalidate/drain builders so none can publish into the reset engine.

## Ordered writes without holding the reader mutex across WAL I/O

1. Acquire the writer gate and recheck writable/fenced state.
2. Under `Engine.mu`, decide a bounded batch against head plus staged decisions. Preserve
   input order, including samples redirected to the same overflow identity.
3. Materialize/copy everything needed after unlocking; produce owned WAL frames.
4. Release `Engine.mu`, retain the writer gate, and write the WAL.
5. On logging failure, apply nothing. Preserve the WAL's short-write repair protocol.
6. On success, apply under `Engine.mu`, then report exact accepted/rejected counts.

Replica application, prune installation, reload identity registration, reset, and head trimming
must join the writer discipline when they could invalidate staged admission or identities.
Simply unlocking the existing method around `WriteFrames` is unsafe.

After WAL success, cancellation cannot discard application. Complete the local apply; a client
that loses its response may have an ambiguous result and retry. Cluster acknowledgements retain
their current quorum rule. This change frees readers during WAL I/O; it does not parallelize
one engine's admission/WAL order. Batching remains necessary.

Keep per-series OOO watermarks across flush. Cardinality, byte limits, overflow redirection,
sample weights, and partial-success accounting must match the existing staged admission model
(`engine/walbatch.go:60`). An earlier successful bounded chunk remains successful if a later
chunk fails; adapter reporting must preserve the established partial-failure contract.

## Flush boundary, durable publication, and failure handling

Retain one frozen generation per engine initially. Hold the writer gate across WAL sealing
and head detachment: no accepted write can straddle them. Subsequent head writes use segments
beyond the captured sequence and the next epoch. Keep the frozen data in the read view.

Build columns, identities, sidecars, and part manifests off-lock. Under the publication
coordinator, derive a candidate catalog from the current committed catalog and operation delta.
Perform CAS and conflict reload off `Engine.mu`. Rebase preserves foreign entries, wants,
holes, allocation lineage, writer-specific epoch slots, and ownership terms. Recheck rollup
compatibility after every rebase. Adoption must prepare identity state before exposing parts.

After durable CAS, install the new read view under `Engine.mu`: remove frozen data and add
flush parts together, or replace merge inputs together. Before that installation, local readers
still see the old complete representation. Exclude reloads that could install new parts beside
the old frozen data. Published part handles must remain immutable; finalize metadata before
installation rather than mutating handles already visible to readers.

Successful CAS requires installation even if the caller cancels. A timeout with uncertain CAS
outcome requires reconciliation before retry, reattachment, or output deletion. An error alone
does not prove the durable commit failed. Before a confirmed commit, failed flush data stays
queryable and retryable; after commit, do not reattach it as unflushed records.

Checkpoint only the sequence captured at detach, after publication. `SegmentWriter` has no
internal concurrency protection: append, seal, sync, healing, checkpoint, and close remain
serialized by the writer gate. Moving deletion outside it requires a separate WAL ownership
change that transfers a closed, durably covered segment prefix to a reclaimer.

Record recovery skips committed epochs; metrics also rely on merge deduplication. Epochs are
writer-specific: a foreign writer's higher number cannot cover this writer's WAL. Preserve
`wal/ARCH.md:170` and `:185`; never checkpoint the current sequence after an off-lock build.

## Snapshot ownership and identity generations

| Snapshot | Contents and lifetime |
|---|---|
| Identity | Fixed-length arena views plus references valid within their captured registry generation. |
| Maintenance | Frozen samples or pinned inputs, identities, source lineage, ownership term, epoch, sealed WAL sequence. |
| Read | One coherent selection of active-head data, frozen generations, and referenced parts. |

Acquire references before unlocking; release them on iterator close and every error path.
Borrowed identity bytes remain immutable. Do not pool backing arrays while views survive.
`View.At` may reuse caller scratch, but retained results cannot alias scratch overwritten by
the next iteration. A snapshot of arena slices does not make the mutable hash lookup safe.

The smallest next change captures `series.View` once, resolves references in bounded `RLock`
chunks while existing maintenance exclusion prevents registry replacement, and encodes outside
the lock. Allocate output storage outside critical sections. This bounds each lock hold while
retaining the current ordering proof. Another correct option scans a captured view off-lock
against detached IDs, but its cost includes dormant identities; measure before choosing it.

A constant-time full detach needs identity references or an immutable lookup owned by the
memtable generation. A bare `uint32` in sample buffers is insufficient because prune renumbers
references. Use generation ownership or stable handles, and measure their resident cost.

Initially exclude prune from overlapping builds. Later, a prune may rebuild off-lock and
validate generations before installation, preserving active/frozen data, readable parts,
concurrent registrations, and revived series. Today's `head.swapIdentity` rechecks only active
buffers because `flushMu` prevents detach/publication; remove that assumption explicitly.

Preserve per-fetch consistency. Sharing a view across PromQL subqueries needs an explicit
query-scope API. Per-fetch snapshots do not imply cross-engine or cross-tenant transactions.
These are in-process snapshots, not a proposal for backup snapshots or copying the entire head.

## Budgets, scheduling, and replication

Charge active and frozen samples, staged requests, builder scratch/output, and pinned identity
arenas to explicit memory budgets. Metric admission currently checks `head.bytes`
(`engine/walbatch.go:77`); extra frozen generations cannot be introduced without accounting.
Reserve capacity for a pressure flush so a large merge cannot consume all memory needed to
flush. Bound queues by bytes and tasks; preserve deferred-tenant fairness.

The primary remains the admission authority and replicates its logged accepted payload;
secondaries apply it without independently rejecting OOO/cardinality cases
(`engine/engine.go:1487`). Snapshots do not make secondary heads durable or alter quorum rules.
Carry the planning ownership term into every builder and recheck it on publication. Lost
tenure invalidates publication even when output objects are complete. Replica refresh retains
uncovered head data and existing per-series coverage/rollup semantics.

## Implementation phases and acceptance tests

1. **Batch first.** Rerun the same pressure fixture; compare accepted/offered rate, queue slope,
   points per WAL operation, CPU and writer wait. Verify identities, reasons, overflow, weights,
   exemplars, policy/rate/sampling behavior, and replay equivalence.
2. **Bound identity capture.** Keep maintenance exclusion. Measure capture, encoding, and
   publication separately; demonstrate bounded individual lock holds as cardinality increases.
3. **Separate writer ordering from reader locking.** Block WAL writes deterministically:
   readers proceed, competing admission and seal remain ordered, failed writes apply nothing.
   Extend staged-admission equivalence and existing short-write/crash fuzz tests.
4. **Move durable publication off `Engine.mu`.** Block CAS/reloads and verify ingestion/read
   progress. Inject crashes after output write, after CAS, before installation/checkpoint, and
   during checkpoint. Require no missing acknowledged data and exact record multiplicity.
5. **Overlap one flush build with one merge build.** Reserve/pin inputs and retain one publisher.
   Pause a merge, trigger pressure flush, and prove flush completes before releasing the merge.
   Exercise both publication orders and verify every unselected/new part survives.
6. **Evaluate further concurrency only from measurements.** Multiple flushes require a
   contiguous committed-epoch frontier: generation 2 finishing first cannot checkpoint past
   unfinished generation 1. Sharded writers additionally need shared cardinality reservations
   and explicit per-series/overflow ordering.

Use channels and fault hooks, not sleeps, for prune plus revived-series races; readers held
across merge/retention; reset during build; tenure loss between plan and CAS; CAS conflict with
foreign additions; non-nesting rollup rebase; replica refresh during flush; and cancellation
immediately after WAL/CAS success. Run race tests for both metric and record engines.

Measure total resident memory and physical bytes including identities/sidecars, not just
column bytes. Increasing throughput while frozen-generation, merge, or remote-write queues
grow is not a completed capacity improvement. Report gains only after controlled reruns.

## Primary references and limits of the analogy

- [VictoriaMetrics partition source](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/lib/storage/partition.go):
  `GetParts` pins references under `partsLock`; `isInMerge` reserves inputs; bounded workers
  handle memory/small/big parts. Borrow reservation and retirement, not its buffered visibility
  or durability contract. Its backup hard-link snapshots are a separate use of immutable parts.
- [ClickHouse query snapshots](https://clickhouse.com/blog/clickhouse-release-25-06): queries
  reference immutable parts, replacements leave old readers valid, and shared query snapshots
  avoid separate subquery views. This is the closest part-lifetime/publication precedent.
- [Loki checkpoint design](https://grafana.com/docs/loki/latest/community/design-documents/2020-09-write-ahead-log/)
  and [WAL operation](https://grafana.com/docs/loki/latest/operations/storage/wal/): rotate before
  covering an older prefix; bound replay memory. Do not copy documented disk-full durability
  exceptions: this engine already rejects failed logging before application.
- [Mimir ingest storage](https://grafana.com/docs/mimir/latest/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/):
  durable ingestion and TSDB consumption are separated, with explicit freshness/read-after-write
  consequences. Kafka or acknowledgement before local apply would be a separate redesign.
- [Tempo configuration](https://grafana.com/docs/tempo/latest/configuration/) and
  [compaction](https://grafana.com/docs/tempo/latest/operations/compaction/): block completion,
  snapshot retention and delayed reclamation are explicit. Borrow lifetime rules, preserving
  local references and existing distributed retention rather than copying timeout defaults.
- [Grafana data sources](https://grafana.com/docs/grafana/latest/datasources/): Grafana queries
  backing systems. Use it to validate freshness and latency; it supplies no engine lock model.

References were read live on 2026-10-27; `main`/`master` source and `latest` docs can change.
