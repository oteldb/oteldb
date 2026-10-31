# 10k hosts after metric consumer reuse, 2026-10-31

The allocation reduction survives the full workload, but 10k remains unsustainable.
Run `profile-20261031T122517Z` uses application `b0f66553` from
[PR #1445](https://github.com/oteldb/oteldb/pull/1445), image
`local/oteldb:metric-consumer-b0f66553`, storage `e9ada72`.
Storage's production implementation is identical to the previous `58a975d` image;
only documentation/testdata cleanup differs. Application runtime changes are the
consumer's existing native-descriptor pool, not a receiver or WAL redesign.
The prior application source also differs by a go.sum tidy; go.mod is unchanged.
The monolith still uses Collector/pdata remote-write ingestion.

Matching binary SHA-256:
`364096ee9e02d52bd42103ec333ad419b917810160cef96aa68eee507bf7a9bc`;
ELF build ID `95f80ae8071edcb6a671844039ffd9529e8c9e0f`.

Fresh volumes, 360s ingest, native file backend/WAL, 10s flush, and identical
node-exporter/nginx/vmagent fan-out. Each database has 8 CPU quota and 16 GiB hard
memory/no-swap limit, GOMAXPROCS=8, GOMEMLIMIT=14GiB. Capture timing matches the
previous repeat, including 3s traces at 120s/300s. This run also saves a freshness
gate at the end and runs queries for ready servers after the heap capture and
stopping vmagent. Outstanding native flush/merge work continues during that pause.

## Throughput, memory and unfinished work

| Observation | Previous repeat | Consumer reuse |
|---|---:|---:|
| Final rate window | 77.8s | 78.5s |
| Offered points/s | 966,118 | 966,308 |
| Accepted points/s | 583,195 | 839,974 |
| Native queued bytes, beginning → end | 517 → 734 MB | 563 → 624 MB |
| VM queue at end | 0 | 0 |
| Accepted points after pause | 239.98M | 253.17M |
| Flushed points after pause | 117.80M | 118.22M |
| Active + detached unflushed points | 122.17M | 134.94M |
| Active points | 10.11M | 99.73M |
| Detached points | 112.06M | 35.22M |
| WAL | 9.88 GB | 11.28 GB |
| Forced-GC heap after pause | 12.46 GiB | 11.03 GiB |
| Retained full-column builder | 3.34 GiB | 1.05 GiB |
| Steady allocated bytes / 30s | 45.50 GiB | 31.21 GiB |
| Steady sampled CPU in GC | 50.4% | 48.2% |
| Historical minimum samples/host | 1 | 3 |

The final rate is higher, but total accepted points increase only 5.5%, and flush
completion barely changes. Prior identical-binary repeats already varied widely in
their final-window rate. These single runs do not estimate a 44% causal throughput
gain. The new queue still grows, accepted rate trails offered rate, and more samples
remain unflushed. No sustainable 10k capacity is established.

Memory reaches the 16 GiB cap, with 117,310 cgroup limit hits and no OOM. The first
maintenance pass takes 72.26s versus 69.93s previously; the next is unfinished at
capture. Shutdown's watchdog still exits with code 1. Every benchmark container is
stopped and its volumes retained; the unrelated navidrome container is untouched.

Heap differences also reflect a different flush phase. The builder contains exactly
`35,215,244 detached points × 32 bytes = 1.0495 GiB`, instead of the prior run's
112M-point output. This cannot be attributed to consumer reuse removing the builder.
The current heap still contains 3.19 GiB head maps/buffer objects, 2.38 GiB sample
arrays, 1.54 GiB series index, 0.79 GiB identity clones, 0.53 GiB watermarks,
0.52 GiB postings, and about 0.57 GiB identity capture.

## Profiles and contention

In the steady window, pdata-to-native allocations fall from 18.70 to 7.95 GiB;
RW-to-pdata allocations are 14.80 versus 16.15 GiB. Metric/point descriptor growth
disappears from the allocation leaders. Conversion still represents 72.9% of allocated
bytes (47.4% RW-to-pdata, 25.5% pdata-to-native); attribute conversion remains large.
Allocated volume also depends on delivered requests and merge/flush phase. The six
isolated before/after samples in PR #1445 establish the per-request allocation win;
these full-run captures show that the reduction persists under this workload.

At 240s and 300s the pressure flush waits in `reserveFlush` / `memlimit.Pool.Acquire`
while maintenance executes a 16-part streamed merge. The latter snapshot reports a
minute of waiting. Fixing receiver churn does not give a large frozen generation a
build reservation while that merge owns the pool.

The 3s traces show about 8.17s and 13.48s aggregate writer-gate delay across goroutines.
Completed filesystem syscall delay is 146ms and 111ms, including 78ms and 51ms in
fsync. These sums are neither wall time nor CPU percentages; waits spanning trace
boundaries are censored. WAL batching has not changed, and writer contention remains.

## Query outcomes

Both servers contain all 10,000 hosts, but native has only three historical samples
per host in the 120s gate versus VM's eight. The required minimum is six, so native
dashboard latency is deliberately not scored on incomplete data.

VM completes all 190 selected requests per mode without errors or empty results.
Random-host p50/p95 is 2.85/9.18ms; repeated/cached p50/p95 is 0.94/1.71ms.
Random selected requests use `nocache=1`; storage/index/OS caches remain warm.
The full 242-query otelbench dashboard completes both modes: 2,420 random-host
targets across ten hosts, then 242 repeated targets with ten measured runs and two
warmups. Per dashboard, 218 targets are nonempty and 24 concern unavailable default
exporter collectors. Otelbench has no explicit nocache option; random target strings
vary by host. Source/raw dashboard reports are retained locally.

Next storage priority remains bounded flush construction and a guaranteed workspace
for flush progress, then a generation-owned memory representation that avoids the
second full sample layout. The requested Astra WAL/head design is in PR #1445;
no coordinator or storage format changes were included in this run.
