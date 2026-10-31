# Metrics ingest reprofile, 2026-10-31

Fresh paired 10k-host run using application `3e7d95a5` (production source `9cd2b49b`),
published storage `v0.50.0`, and image `local/oteldb:metrics-reprofile-3e7d95a5`.
Each database has 8 CPU quota, 16 GiB memory, no swap; oteldb uses file/WAL storage,
10s flush and `GOMEMLIMIT=14GiB`. The source, binary, full configuration, image ID,
CPU/heap/delta-allocation profiles and cgroup snapshots are retained in
`results/profile-20261031T013009Z`. An admin listener adds low-frequency live stats.
No production ingestion route or storage implementation changed.

## Capacity and lifecycle

The final 79.2s ingest window offered **965,652 points/s** and oteldb accepted
**780,892 points/s**. Its remote-write queue grew **550 → 639 MB**; VM's stayed empty.
Neither persistent queue dropped bytes. The historical minimum was **2 samples/host
for oteldb versus 8 for VM**, so oteldb receives no query-latency score.

This six-minute trial had no OOM. The cgroup nevertheless reached **16.00 GiB** and
recorded 148,744 memory-limit hits. Sampled peak process RSS was **14.08 GiB**.
CPU throttling accumulated only **0.179s**; the quota was not continuously saturated.
This is another overloaded run, not a demonstrated improvement over the earlier trial.

Only **121.36M** of **249.72M** accepted points had flushed after the brief pause.
Live buffers held **91.09M** points and a detached flush held approximately **37.27M**.
Their combined logical sample size was **2.05 GB**, with **6.59 GB** of WAL remaining.
The first maintenance pass took **70.3s**; the second was still running at the last
observation. A zero completed-cycle counter earlier meant an active, unfinished pass.
The SDK shutdown watchdog subsequently exited oteldb with code 1. All containers
are stopped; the queue and data volumes remain available.

## CPU and allocation

Two 30s profile windows separate startup from steady overload. Categories below
are disjoint stack classifications; cumulative frames are not added together.

| Stage | Early allocation | Steady allocation |
|---|---:|---:|
| Prometheus remote write → pdata | 35.4% | 34.0% |
| pdata → native metrics | 40.3% | 39.6% |
| Merge | 6.5% | 19.3% |
| Flush | 12.5% | <0.1% |
| Append/WAL | 3.6% | 4.8% |
| Other | 1.7% | 2.3% |

The early window allocated **52.15 GiB/30s** and steady overload **40.86 GiB/30s**.
GC consumed **50.9%** of steady sampled CPU (66.36 CPU-seconds within the 30s wall
window); append/WAL consumed 15.9%, merge 8.7%, and flush 3.4%.
The process allocated about **608 GB cumulatively** by the final observation.

After stopping vmagent and waiting 15s, a forced-GC heap still retained **12.10 GiB**:

| Allocation site | Retained GiB |
|---|---:|
| Head maps/sample-buffer objects (`head.bufFor`) | 3.30 |
| Sample-array backing storage | 2.41 |
| Series index | 1.56 |
| Flush output columns | 1.15 |
| Attribute/identity clones | 0.81 |
| Persistent per-series OOO watermarks | 0.56 |
| Label postings | 0.51 |

Workers were still processing frozen/live heads. This is a retained-work measurement,
not an idle leak test or a promise that all those allocations can be removed.

## Ownership and the pressure loop

`engine/head.go` keeps a map from canonical 128-bit series IDs to `*sampleBuf`.
Each buffer is a 72-byte three-slice header before its backing allocations and map
overhead. At the observed 14.49M series, millions of short per-series buffers create
many GC-visible objects. The series index, postings and per-series newest timestamp
map survive flushes independently.

`head.detach` swaps in an empty samples map. The old generation stays readable until
publication; writes allocate new maps/buffers while a flush retains the detached one.
`Engine.flush` detaches before `reserveFlush` acquires its build grant. Goroutine
snapshots show that pressure flush waiting in `memlimit.Pool.Acquire`, while a merge
finishes its identity capture. Large estimates exceed the default 1.75 GiB build pool
and are clamped to the whole pool, so the nominal flush reserve cannot admit them
beside the merge. Incoming heads/WAL continue growing during that wait.

`buildFlushColumns` additionally copies each sorted per-series window and grows its
three output columns from zero capacity. Ordered merge identity capture uses bounded
256-ID read-lock sections; snapshots show it reacquiring that lock under concurrent
writer traffic. These details connect allocation churn to delayed flush progress.

## Existing native conversion path

The measured `cmd/oteldb` route is Collector `prometheusremotewritereceiver` →
`prometheusremotewrite.FromTimeSeries` → `storage/otlp/pdataconv`. It does not use
`internal/promrw.Converter`. `cmd/odbingest` already constructs `promrw.NewHandler`
and uses that native converter instead.

The existing `internal/promrw/BenchmarkConvert` compares both paths on the same
decoded recorded corpus. Six 500ms samples, eight Go processors, AMD Ryzen 9 9950X3D2:

| Path | Time/request | Bytes/request | Allocations/request |
|---|---:|---:|---:|
| pdata → native | 1,036.3 µs | 2,744,969 | 57,180 |
| Reused Direct converter | 219.2 µs | 487.5 | 0, rounded |

Benchstat: time −78.85%, bytes −99.98%, both p=0.002, n=6. Direct's initial arena growth
is amortized into the reported bytes. This excludes HTTP, protobuf unmarshalling,
WAL, flush and disk, and is not a direct-ingest end-to-end capacity result.

## Follow-ups

1. Use the existing native converter at a native-ingest front door, preserving configured
   Collector pipeline behavior. It targets the two stages responsible for 73.7% of
   steady allocation; validate with a complete paired ingest run.
2. Store head samples in dense, reusable generations keyed by the existing dense series
   reference. Keep canonical hashes at durable boundaries and preserve reader/rollback
   lifetimes; release/reuse a frozen generation only after its users finish.
3. Make large flush work fit a real bounded grant, so a detached head cannot wait for
   whole-pool admission while its replacement grows without bound. Admission, active
   head memory, frozen generations and encode scratch need separate accounting.
4. Pre-size flush columns and avoid redundant copies of already-sorted immutable full
   windows. Revisit ordered merge identity capture's repeated lock acquisition.

These are measured next steps; this profiling change implements none of those
production optimizations. Selected profiles, counters and conversion benchstat are
committed separately from the capture harness.
