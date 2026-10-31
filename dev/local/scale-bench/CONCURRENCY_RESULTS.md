# Snapshot and maintenance concurrency

Implementation and write-pressure validation are complete in the sibling storage worktree `concurrent-storage`, branch
`claude/snapshot-concurrency`, on the validated compact-identity and 4,096-point batching baseline.
The earlier [Astra design](CONCURRENCY_DESIGN.md) remains the design record. The batch-size
sweep is in [BATCH_SIZE_RESULTS.md](BATCH_SIZE_RESULTS.md); it did not justify changing the cap.

## Implementation

- Identity capture snapshots immutable arenas. Dense frozen maps (at least 25% of registry rows)
  scan that view off-lock against the existing map; sparse and ordered callers resolve references
  in bounded 256-ID read-lock sections. Encoding and output-map allocation occur outside locks. Maintenance guards
  prevent registry replacement while snapshots are live; an arena view does not make maps safe.
- An engine guard separates catalog ownership, ordered mutation/WAL ownership, and reader-visible
  state. Ordinary writers bypass catalog ownership. WAL encoding/admission stay under state;
  blocking WAL I/O retains writer ownership while admitting readers and derived-cache updates.
  Seal, checkpoint, sync, close, and replica application obey the same ordering.
- Publication builds a candidate part set and captured epoch without exposing it to queries.
  CAS, conflict reload, and foreign object reads admit ingestion while retaining catalog ownership.
  Identity registration and read-view installation remain protected. Full recovery/reload retains
  writer ordering because discovering a new local WAL epoch cannot race new writes into an old one.
  A reload during an in-flight flush preserves the already-sealed next epoch. Public reloads and
  pruning share an identity-lifecycle gate, while owner orphan sweeps skip active local builds;
  reloads can still fence a delayed build.
- An ambiguous CAS response is confirmed by reading back the exact candidate under a bounded context
  that outlives request cancellation. Unconfirmed outcomes retain output objects and WAL. Publication
  installs one read view before checkpointing only the sealed prefix captured at detach.
- Engine overlap permits one flush build and one merge build, retains input pins through publication,
  rebases replacements on the current part set, and excludes reset/prune/repair appropriately.
  A shared build pool reserves one quarter for flush progress; oversized requests serialize. The facade has a
  separate bounded pressure-flush worker and drains its lifetime on Close. Opaque side stores keep
  serialized per-engine builds because their interface does not bound encoding/storage scratch.

Memory charges are conservative admission estimates, not an allocator or hard RSS limit. They
separate columns/sorting/maps/sidecars, serialized identity references, symbol encoder workspace,
and materialized attributes (including nested values and long shared context). Metric materialization
reuses one scratch; records may retain per-stream materialization. The reserve cannot make an
individual oversized operation fit a smaller process, so such operations must run alone.

## Index validation retention

The prior write-only heap attributes about 1.33 GiB to buffers loaded by `openPagedIndex`. Large
parts kept a full validated index until the first query released it; a part built but never queried
could retain the buffer indefinitely, independently of cache eviction. Validation also populated
the whole-object cache even when later point queries needed only one page.

Durable range-readable validation now bypasses that cache. Large paged indexes retain only fences
after the full CRC/structural check; first point probes read at most one page. Disk formats and CRC
checks are unchanged. Small/non-ranged indexes retain their fallback behavior. Cached-file and bare
file tests reproduce the old retention and pass with the fix. The entire 1.33 GiB is not a promised
RSS reduction: transient reads and other cache users still consume memory.

## Validation evidence

`results/concurrency-validation/` contains the E2E-gated native HTTP publication regression
(run with `E2E=1` and the worktree module replacement): baseline fails
because a write waits behind gated catalog CAS; the implementation passes writes and a PromQL
count of exactly two points before/after CAS and after clean reopen. Existing native identity and
WAL shape-equivalence cases also pass. Focused index-retention race checks pass.

The final full storage suite, affected engine/helper/WAL race suites, application native-file HTTP
race tests, full storage lint, and diff-check pass. Raw logs are retained in the evidence directory.
Correctness tests alone are not used as evidence of capacity.

## First live concurrency candidate (b17)

The preserved batched image was OOM-killed at 248s in a fresh 10k-host run. The b17 concurrency
image was OOM-killed at 230s under the same 8-vCPU/16-GiB limits. At a near-common 214s observation,
baseline accepted 132.59M points and had completed flushes covering 50.14M; b17 accepted 182.83M but
had completed only one 3.99M-point flush. This is faster admission without adequate drainage,
not higher stable capacity. Both runs are truncated; no clean five-minute throughput comparison
is claimed. Docker OOM state and the last healthy sample boundaries are preserved.

The sampled 5s trace attributes 3.43s of the pressure worker's blocking to `RWMutex.RLock` in
`Capture -> identitiesOf`, after flush memory admission. It rules out a pool wait for this sampled
stall. The 256-ID chunks repeatedly reacquire a writer-preferring lock behind 4,096-point writers;
the estimate and final identity map each perform a lookup pass. This spreads short lock holds but
can greatly delay flush completion under sustained writes. The 180–210s CPU profile used 5.55 cores;
background marking accounted for 36.35% and GC assists 23.20% cumulatively. They must not be added to
other overlapping cumulative entries.

The 1k-host b17 dashboard/restart check passed: random p50/p95 = 2.11/22.36 ms, repeated = 1.26/2.90 ms;
VictoriaMetrics measured 1.40/2.89 ms and 0.87/1.22 ms. All 190 requests/mode in the 19-query subset
returned data without errors. Both full 242-target dashboards completed through otelbench.
Evidence: `results/20261028T164252Z/`, with omitted-collector empty panels recorded.

The b17 image and exact patch are retained as failed-candidate evidence. Capture-size contention
benchmarks and a corrected-candidate rerun are in progress; the metric append cap stays 4,096.

## Capture correction

A contention benchmark with four real 4,096-point WAL writers confirmed that increasing lookup
chunks improves maintenance progress but consumes writer time. The selected correction instead
scans an immutable registry view against the existing frozen map when density is at least 25%.
That scans at most four times the selection size and allocates no extra ID set. Sparse lookups
and the ordered `Capture` contract are unchanged; refs are still allocated only after admission.

Six paired repetitions on NVMe-backed WAL, GOMAXPROCS=8:

| Registry / selected IDs | Chunk lookup | Immutable scan | Difference |
|---|---:|---:|---:|
| 1,048,576 / 1,048,576 | 5.834 s | 319 ms | -94.5%, p=.002 |
| 1,048,576 / 262,144 | 1.448 s | 125 ms | -91.4%, p=.002 |

Writer throughput did not change significantly in either paired comparison. Full-density writer
p99 increased from 12.00 to 12.64 ms; quarter-density p99 fell from 11.81 to 6.48 ms. These are
observations during different capture-duration windows, not a general writer-latency guarantee.
The benchmark includes both lookup passes; allocation counts also include background writers.
Reports and raw samples are in `results/concurrency-validation/capture-*.benchstat.txt` and
`capture-*.bench.txt`. Dense/sparse equivalence, concurrent registration, callback ownership,
engine/record-engine race suites, and full lint passed after the correction.

The corrected live run completed; its results follow. The failed b17 observation remains part of
the record and is not used as evidence that the implementation supports 10k hosts.

## Corrected 10k-host run

Image `local/oteldb:scale-concurrent`, ID
`sha256:c1df1d2e37d29fb952dd9261a88ae461a23f834f7d82e45b58192f3da82c0d1d`.
Storage base `9765fda843d16b52d8bd40ff854e8e9412e11cc6`; the complete image source-diff SHA256 is
`6f5b4afd7160686c8eba9e285ab82778ed0e51dd11f0f94646c654d50981bdf6`, matching
`results/concurrency-validation/implementation-at-build.patch` exactly. The oteldb checkout is
`e5de6b841f45e167ec484be59c86cfecdfbbd7ef`; its dependency pin is preserved via a build-only modfile.

Each run used fresh volumes, native file storage, 10k nginx-fanned targets, 15s scrapes, 8 vCPU,
16 GiB hard cap/no swap, and Go limit 14 GiB. Only oteldb received writes and no queries ran during
the offered load. These are one diagnostic run per image on a shared host, not repeated capacity
trials. The corrected run additionally captured non-forced-GC heaps at about 90 and 180s.

| Run | Outcome | Last healthy metrics | Accepted points at that boundary |
|---|---|---:|---:|
| Preserved batched control | OOM-killed at 248s | 244.6s | 143.23M |
| b17 / chunk lookup | OOM-killed at 230s | 213.9s | 182.83M |
| Corrected immutable scan | Completed 300s, no OOM kill | 300.0s | 215.05M |

The boundaries differ, so these totals do not establish a throughput speedup. At a near-common
214s observation, control/b17/corrected accepted 132.59M/182.83M/147.23M, while completed flushes
covered 50.14M/3.99M/91.59M. The correction improves drainage rather than maximizing admission
while unflushed state expands.

The corrected run offered 290.17M points (~967k/s), accepted 215.05M (~717k/s), and left **496.85 MB
queued**. Six completed flushes covered 129.19M points and one merge completed. Thus a substantial
amount was still in active/frozen or in-progress flush state; the queue was not bounded at the
input rate. This does **not** establish sustainable 10k-host capacity. No larger scale was rerun.
Flush duration sums include queued concurrent callers and must not be treated as exclusive wall time.

Peak cgroup usage still reached about 16 GiB, with ~124k max/reclaim events and no OOM kill. Mean
CPU was about 4.62 cores with 0.49s cumulative throttling. In the sampled 5s trace, identity capture
was no longer a material blocking stack. The 30s CPU profile used about 4.51 cores; background GC
was 31.4% and GC assists 9.0%, versus 36.4% and 23.2% in b17. The 180s heap remains dominated by
per-series head buffers, compact identities/postings/watermarks, sample arrays, and flush columns.
Those remain memory/maintenance targets; raising the metric append cap is not supported by the
separate batch-size sweep.

After requested stop, the corrected run still exceeded the application's graceful-shutdown
watchdog and exited 1 with `OOMKilled=false`. The 10k WAL/parts volumes are retained; this is not
a successful 10k clean-restart test. The separate 1k dashboard check validates clean restart.

Raw runs: `results/write-20261028T162434Z/` (control), `write-20261028T163354Z/` (b17), and
`write-20261028T171719Z/` (corrected). Counter derivation, exact sample boundaries, image IDs and
terminal states are in `results/concurrency-validation/write-comparison.json`; `summarize.py`
recreates it. CPU/sync profiles and early/steady/loaded heaps are retained with each available run.

## Corrected 1k-host dashboard and restart

Both servers passed the all-host/history gate. All 190 requests per mode in the 19-query subset
returned nonempty results without errors. The full 242-target dashboard completed through
otelbench in random and repeated modes, with the same 24 missing-collector targets empty in both
engines. Oteldb preserved all 1,000 hosts across restart.

| Engine | Random p50 / p95 | Repeated p50 / p95 |
|---|---:|---:|
| Corrected oteldb | 2.26 / 21.46 ms | 1.36 / 3.64 ms |
| VictoriaMetrics | 1.43 / 2.57 ms | 0.91 / 1.28 ms |

These are separate short live datasets; the small median differences from the preceding 1k trials
are not a statistically established change. Random-query tails remain materially higher than
VictoriaMetrics. Random hosts do not flush OS/index/column caches, and repeated oteldb requests
exercise storage caches rather than a proven full-response cache.

[Final dashboard report](results/20261028T172424Z/REPORT.md) includes per-query timings, raw requests,
resource samples, restart proof, and otelbench YAML/logs. All benchmark containers are stopped;
10k data volumes remain retained. Measurements predate the review commits. The implementation
is now published as the draft stack [storage#791](https://github.com/oteldb/storage/pull/791),
[storage#792](https://github.com/oteldb/storage/pull/792), and
[storage#793](https://github.com/oteldb/storage/pull/793). The stack was rebased onto dependency-only
main updates; full tests and lint passed after rebase. The downstream draft pins that feature commit.
