# Native metric batching

The native storage facade now coalesces whole small metrics into engine appends of at most
4,096 points, within one tenant and one `WriteMetrics` call. This addresses remote write's
one-metric-per-series shape without regrouping descriptors or changing canonical identities.
Input order and WAL-before-apply remain intact. Existing metrics larger than the bound are
appended intact, preserving their failure atomicity. No concurrency changes are included.

The accumulator copies projected ID/timestamp/value vectors and holds a three-pointer borrowed
identity source for each metric. Projection buffers can be reused safely; sources are cleared
before pooling. Input metrics remain owned by the synchronous caller throughout the write.
Rate, cardinality, in-flight limits, and enabled sampling retain their original per-metric path.
Disabled sampling still observes every metric, including policy transitions and a previously
active sampling window. Structural part limits do not disable batching.

## Disk-backed microbenchmark

Same 1,024-point fixture, six repetitions of 100 iterations, NVMe-backed temporary directories,
`GOMAXPROCS=8`, `GOMEMLIMIT=14GiB`, no concurrent maintenance. The baseline is the compact
implementation before batching, not the original unoptimized image.

| Remote-write fixture with WAL | Before | Batching | Improvement |
|---|---:|---:|---:|
| One metric name | 1.273 ms/op | 0.498 ms/op | 2.55x |
| 128 metric names | 1.260 ms/op | 0.495 ms/op | 2.55x |

Both comparisons have `p=0.002`, `n=6`. WAL appends fell from 1,024 to one for both shapes.
Replayed samples, canonical IDs, and WAL byte counts remain equal; homogeneous inputs also
produce identical WAL bytes. Batching reduces operation count, not identity/WAL payload bytes.
Allocated bytes increased about 6% and allocation count about 0.4% for these durable fixtures;
this is not an allocation optimization. See `results/batching-validation/benchstat.txt`.

## Live 10k-host write pressure

Fresh file-backed volumes, 15s nginx-fanned scrapes, only oteldb receiving writes, no PromQL
queries, 300s duration, 8-vCPU quota, 16-GiB cap, no swap, Go limit 14GiB. Both runs offered
about 290.16M points, approximately 967k/s. Profiles and debug instrumentation were enabled
in both runs. These are one diagnostic run per implementation, not a sustained capacity claim.

| Measurement | Compact baseline | With batching |
|---|---:|---:|
| Accepted points | 75.00M | 154.92M |
| Accepted points/s | 250k | 516k (2.07x) |
| WAL appends | 75.04M | 48,385 |
| Accepted points per WAL append | ~1 | ~3,202 |
| Process write syscalls | 75.00M | 115,539 |
| Pending remote-write bytes | 1.435 GB | 0.906 GB |
| Mean CPU cores | 2.39 | 3.59 |
| Peak cgroup memory | ~16 GiB | ~16 GiB |
| OOM kills | 0 | 0 |
| Completed flushes / total seconds | 14 / 144.5 | 9 / 206.2 |
| Completed merges / total seconds | 9 / 116.5 | 3 / 51.5 |

The queue still grows: this does **not** establish 10k-host readiness. Memory reclaim events
increased from about 120k to 153k. CPU remains below the eight-core quota; throttling totals
were only 0.04s and 0.41s. Completed maintenance counters exclude in-progress operations.
Head-shape gauges are refreshed by maintenance and may be stale during a long operation.
Both heavy-load runs exceeded the application shutdown watchdog after the requested stop and
exited with code 1 (`OOMKilled=false`); neither proves a clean shutdown at 10k. Post-stop
logs and container states are saved with each run. Their WAL volumes remain retained.

The five-second traces show aggregate `Engine.AppendBatch` mutex wait falling from 28.57s
to 7.77s. This is waiter-side delay in two sampled intervals, not per-request latency or a
complete attribution to lock holders. The new 30s CPU profile used about 2.84 cores; background
GC accounted for about 28.6% of samples and flushing for about 27.6% cumulatively. Remaining
maintenance, map lookup, and GC work is material after the syscall amplification is removed.

Total process write characters increased 39.1 to 47.3 GB while accepted points doubled;
backend object writes decreased 20.76 to 18.47 GB. These are different progress points with
ongoing maintenance, not a steady-state write-amplification comparison. Batching does not
remove the WAL identity records needed after a head starts a new buffer.

Raw evidence: `results/write-20261027T204416Z/`; baseline:
`results/write-20261027T184924Z/`. Derived counters and elapsed-time normalization are in
`results/batching-validation/write-comparison.json`. No load above 10k was rerun for this change.

## Live 1k-host dashboard check

A fresh comparison against VictoriaMetrics used the same 8-vCPU/16-GiB limits and nginx fan-out.
Both engines passed the all-host and historical-sample gates. All 190 requests per mode in the
19-query subset returned nonempty results without errors. Native-file clean restart preserved
all 1,000 hosts. The full 242-target dashboard completed through otelbench for both engines in
random-host and repeated modes. Both engines had the same 24 empty targets out of 242
(240 of 2,420 random-host evaluations); these remain visible in the saved output.

| Engine | Random p50 / p95 | Repeated p50 / p95 |
|---|---:|---:|
| Batched oteldb | 2.03 / 21.83 ms | 1.29 / 3.39 ms |
| VictoriaMetrics | 1.48 / 2.75 ms | 1.00 / 1.43 ms |

The preceding compact-only run measured oteldb 1.94 / 21.97 ms random and 1.70 / 3.72 ms
repeated. These are separate short live runs, so the small random-median difference is not a
statistically established regression or improvement. Random requests bypass VictoriaMetrics'
response cache and change hosts, but still benefit from OS/index/decoded-column caches.
Repeated oteldb requests exercise storage caches, not a proven full-response cache.

Evidence: [dashboard report](results/20261027T205012Z/REPORT.md), raw per-query timings,
readiness checks, and otelbench YAML/logs in that run's `1000/` directory.

## Correctness

Full storage tests, focused race tests, full storage lint, and native-file Prometheus HTTP
round-trip tests passed. Coverage includes size boundaries, preserving large metric atomicity,
WAL failure/retry and replay, reused projection buffers across resource/scope groups,
descending timestamp admission across the OOO window, and injected-clock sampling transitions.
The HTTP test flushes/reopens the native store and checks selectors, rates, and label values.
No PromQL semantics were changed.

## Tracking and concurrency design

[oteldb#264](https://github.com/oteldb/oteldb/issues/264), the batching issue found by title,
is closed and concerns the older collector/ClickHouse pipeline. No exact open native-facade
batching issue was found. [storage#635](https://github.com/oteldb/storage/issues/635) tracks
WAL fsync under the engine lock; its existing engine batch support does not combine separate
projector emissions. [oteldb#1106](https://github.com/oteldb/oteldb/issues/1106) concerns the
remaining PRW/pdata/internal conversion overhead.

The requested Astra subagent produced [CONCURRENCY_DESIGN.md](CONCURRENCY_DESIGN.md), with
primary references to VictoriaMetrics, ClickHouse, and Grafana Loki/Mimir/Tempo. Its proposed
order is bounded batching, bounded identity-reference capture into immutable snapshots,
an ordered writer gate, serialized durable publication, then memory-bounded overlapping builds.
It preserves WAL write before apply, durable CAS before local read-view installation, and
installation before checkpointing the captured sealed WAL prefix. Snapshots need explicit
ownership: copying a view does not make a mutable registry map safe to read without locking.
