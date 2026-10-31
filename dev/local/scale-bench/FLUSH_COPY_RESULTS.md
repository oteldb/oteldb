# Metric flush copy optimization, 2026-10-31

[Storage PR #805](https://github.com/oteldb/storage/pull/805) removes temporary per-series
timestamp/value copies for ordered frozen buffers and preallocates final metric columns.
Unordered inputs keep the existing sorting path. Output arrays remain independently
owned; head lifetimes, admission, receiver routing and file formats are unchanged.

## Isolated builder measurement

Six 200ms single-CPU samples, Linux/amd64, Ryzen 9950X3D2, Go 1.27.3 (`nodwarf5`).
The parent is storage `db64b23`; the optimized source is `dc89000`, with generated
measurement files committed separately in `58a975d`.

| Fixture | Time before → after | Allocated MiB before → after | Allocations before → after |
|---|---:|---:|---:|
| 65,536 series × 1 point | 9.349 → 7.403 ms | 12.094 → 3.000 | 131,153 → 5 |
| 65,536 × 8 ordered | 17.888 → 8.205 ms | 91.87 → 17.00 | 131,180 → 5 |
| 1,024 × 128 ordered | 1.692 → 0.320 ms | 22.742 → 4.016 | 2,138 → 5 |
| 65,536 × 8 weighted | 21.445 → 8.839 ms | 115.98 → 21.00 | 196,750 → 6 |
| 65,536 × 8 unordered | 24.73 → 17.50 ms | 103.87 → 37.00 | 196,716 → 196,613 |

All differences have p=0.002, n=6. Map/input construction is excluded; every builder
call allocates its own output. The high-cardinality ordered fixture takes 54.1% less
time and allocates 81.5% fewer bytes. Unordered sorting scratch remains.

## Paired 10k diagnostic run

Image `local/oteldb:flush-copy-58a975d`, application `09bbd6c1` (runtime source
`9cd2b49b`), local storage override `58a975d`. This includes no pending retention
optimization and no native receiver rerouting. Each database has the same 8 CPU quota,
16 GiB cap, no swap, 14 GiB Go soft limit; file/WAL, 10s flush, and fresh volumes.
Profiles and metrics use the same harness as `METRICS_PROFILE_RESULTS.md`.

| Observation | Earlier v0.50.0 profile | Copy-optimized profile |
|---|---:|---:|
| Final measured ingest window | 79.2s | 80.3s |
| Offered points/s | 965,652 | 964,262 |
| Accepted points/s | 780,892 | 701,651 |
| Native queue at window end | 639 MB | 741 MB |
| VM queue | 0 | 0 |
| Historical minimum samples/host, native / VM | 2 / 8 | 2 / 8 |
| Accepted points after the brief pause | 249.72M | 238.61M |
| Flushed points after the brief pause | 121.36M | 147.51M |
| Live + detached unflushed points | 128.36M | 91.10M |
| Forced-GC heap after the pause | 12.10 GiB | 10.37 GiB |
| Peak cgroup memory | 16 GiB | 16 GiB |
| Steady sampled CPU in GC | 50.9% | 55.1% |

This single shared-host profiling trial demonstrates **no end-to-end throughput
gain**. Accepted throughput is lower than the earlier trial, although more points
finished flushing and fewer remained in the heads. Different flush/merge phases make
these observations unsuitable as a controlled estimate of the causal capacity change.
The controlled builder samples above establish the copying/allocation improvement.
Sustainable 10k capacity remains unresolved; no native query latency is scored.

The optimized run had no OOM or persistent-queue eviction. It still hit the memory
limit 153,911 times and left 9.53 GB of WAL; shutdown's watchdog exited with code 1.
The first maintenance pass took 66.75s, versus 70.3s previously; this includes maintenance
work and waiting, and is not an isolated flush encoding duration.

## Remaining allocation work

During the optimized early 30s window, builder allocations total about 0.40 GiB and
no longer include ordered per-series window copies. In its steady window, 54.95 GiB
was allocated: the two conversion stages account for 63.0%, and flush for 30.8%.
Almost all flush allocation in that window is below `writePart`, including identity
encoding, aggregate statistics, watermarks and column encoding. The earlier profile
was mostly waiting rather than executing flush work during its steady window, so
stage percentages alone are not a per-row efficiency comparison.

The final 10.37 GiB heap still includes 3.36 GiB of head maps/buffer objects, 1.58 GiB
of series index, 1.54 GiB of sample arrays and 1.31 GiB of builder arrays/scratch.
Admission and frozen/live head ownership remain the same; eliminating temporary copies
does not eliminate the final column arrays or make flush work bounded.

Full tests, targeted race checks, table-driven ownership/ordering/weight/float-bit
cases and a file-backed weighted close/reopen test pass; lint passes. All benchmark
containers are stopped. Raw matching binary/profiles/volumes remain local under
`results/profile-20261031T020637Z`; selected measurements are committed separately.
