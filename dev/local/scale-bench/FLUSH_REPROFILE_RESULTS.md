# Repeat profile after the flush-copy optimization, 2026-10-31

Run `profile-20261031T113922Z` repeats 10k hosts on the exact same image and binary
as `profile-20261031T020637Z`: `local/oteldb:flush-copy-58a975d`, storage `58a975d`,
binary build ID `72e07bb772ec3ffb95ef22c8252ff85a443f3551`. Binary SHA-256 is
`aa3df3c8a5519e06616cd16023f8f56d9c72aff60c36fdf05f3a0060a8fc2169`.
The receiver still uses Collector/pdata; no further production code changed.

Each database has 8 CPU quota, 16 GiB memory, no swap and `GOMEMLIMIT=14GiB`.
Native file/WAL storage, 10s flush, fresh volumes, the same exporter/nginx/vmagent
fan-out, and 360s ingest. CPU/allocation profiles cover the early and steady windows.
This repeat also captures 3s execution traces around 120s and 300s, outside those CPU
profile windows. Their overhead is an additional limitation of cross-run comparisons.

## Capacity and retained work

| Observation | Previous copy-optimized run | This repeat |
|---|---:|---:|
| Final rate window | 80.3s | 77.8s |
| Offered points/s | 964,262 | 966,118 |
| Accepted points/s | 701,651 | 583,195 |
| Native queue at window end | 741 MB | 734 MB |
| VM queue at window end | 0 | 0 |
| Accepted points after pause | 238.61M | 239.98M |
| Flushed points after pause | 147.51M | 117.80M |
| Unflushed live + detached points | 91.10M | 122.17M |
| Detached points | 36.73M | 112.06M |
| Forced-GC heap after pause | 10.37 GiB | 12.46 GiB |
| Builder allocations retained in heap | 1.31 GiB | 3.34 GiB |
| Steady CPU in GC | 55.1% | 50.4% |

The identical binary accepts nearly the same total while final-window throughput
and completed flush work vary substantially. Flush/merge phases matter; these trials
do not estimate an optimization's causal capacity effect. Both remain overloaded.
This repeat's native queue grows 517 → 734 MB within the measured window, with no
persistent-queue eviction. Freshness is one historical sample/host versus VM's eight,
so no native query latency is scored.

Memory reaches the 16 GiB cap, with 57,682 limit hits and no OOM. CPU throttling is
0.369s. The final WAL is 9.88 GB. The first maintenance pass takes 69.93s and the second
259.02s; these include waiting and multiple stages, not just flush encoding. Shutdown's
watchdog still exits with code 1. All benchmark containers are stopped; volumes remain.

## The remaining full-head allocation is directly explained

The final counters show 239,978,291 accepted points and 117,803,551 flushed points.
Of the remaining 122,174,740 points, 10,110,185 are active and 112,064,555 detached.
The flush builder's mandatory columns hold 16 bytes of series ID, 8 bytes timestamp,
and 8 bytes value per point:

`112,064,555 × 32 = 3,586,065,760 bytes = 3.34 GiB`.

That matches the retained builder allocation in the forced-GC heap. Removing temporary
copies works, but building a full detached generation still allocates an output that
grows with backlog. Disk-part splitting occurs afterward and does not bound this
materialization. This is outstanding work, not evidence of an idle leak.

Other retained terms are head maps/buffer objects 2.84 GiB, sample arrays 1.89 GiB,
series index 1.52 GiB, identity clones 0.83 GiB, per-series watermarks 0.57 GiB,
postings 0.50 GiB, and identity capture about 0.54 GiB.

## CPU, allocation and waits

The steady allocation window totals 45.50 GiB/30s: remote write → pdata 35.5%,
pdata → native 41.1%, merge 15.9%, flush 0.9%, and the remainder other work.
GC consumes 50.4% of sampled CPU. Categories are disjoint stack classifications.

The low flush allocation fraction in that window is not a new efficiency claim:
the 240s goroutine snapshot shows pressure flush blocked in `reserveFlush` /
`memlimit.Pool.Acquire` while the maintenance worker executes a streamed merge.
At 180s pressure flush is estimating identity memory; at 300s it is encoding a
part's identities. At 120s it encodes values while timed maintenance waits on
`lockFlush`. Waiting, building, encoding and publication therefore occur in different
windows and must not be conflated.

The two 3s traces record approximately 8.14s and 8.07s of aggregate goroutine delay
under `engineguard.Mutex.LockWriter` on the append path. These are sums across
goroutines, not wall-clock durations or independent percentages to add to CPU.
Completed filesystem syscall delay totals only 178ms and 114ms in those windows;
Fsync accounts for 111ms and 48ms. These short traces show substantial writer
serialization, without showing filesystem delay as the dominant cost. They do not
capture the full duration of waits spanning a trace boundary; the longer admission
wait is established by goroutine snapshots instead.

The next storage change should bound a flush's selected generation/output before
materialization and make that unit fit its admission reservation. Reusable dense head
generations would address the separate map/object footprint. Native receiver routing
remains the largest measured opportunity to remove conversion churn. None of these
production changes is included in this profiling update.

Raw matching binary, traces and profiles remain local in the run directory. Selected
counter summaries, disjoint stage totals, heap/CPU/allocation tops and trace-derived
wait/syscall summaries are committed separately from the capture harness.
