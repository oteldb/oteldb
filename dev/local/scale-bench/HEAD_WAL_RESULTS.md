# Combined sealed-head streaming and WAL profile, 2026-10-31

The combined build completes substantially more flush work and retains less paused
heap, but still falls behind 10k hosts and creates substantially more live parts.
This is not a sustainable-capacity result.

Run `profile-20261031T133044Z` uses app `cf6b7f4d` (descriptor reuse from PR #1445)
and storage integration `3cfc382e6781c050466c82648ce5b30b0716e081`, combining
[head PR #807](https://github.com/oteldb/storage/pull/807) and
[WAL PR #806](https://github.com/oteldb/storage/pull/806). The integration branch is
published as `claude/635-head-wal-integration`; neither PR is merged by this run.
Image `local/oteldb:head-wal-3cfc382` was checked against the matching host binary:
SHA-256 `c5d96457d8e4ddca53f92ddf0d9a2f002783f77cd09e6c998ec05ae2613dfeb9`,
ELF build ID `0ca4d3649a70273b07777aff6171a08e75f2cc73`.

The previous descriptor-reuse run used storage e9ada72; this integration also
incorporates main's merged retention/fairness PR #804. No partial-retention policy
is configured here, but scheduler changes are an additional comparison limitation.
These runs do not isolate the head and WAL changes from each other or from phase
variation. The separately paired microbenchmarks establish their narrower effects.

## Setup and outcome

Fresh volumes, native file backend/WAL, 10s flush, 360s ingestion, the same pinned
exporter/nginx/vmagent fixture. Each database: 8 CPU quota, 16 GiB hard memory cap,
no swap, GOMAXPROCS=8 and GOMEMLIMIT=14GiB. Same CPU/allocation windows and 3s traces.
The receiver still uses Collector/pdata; no arena or lazy hashing adapter is enabled.

| Observation | Descriptor reuse | Head + WAL integration |
|---|---:|---:|
| Final rate window | 78.5s | 78.2s |
| Offered points/s | 966,308 | 967,272 |
| Accepted points/s | 839,974 | 821,552 |
| Native queued bytes, beginning → end | 563 → 624 MB | 375 → 446 MB |
| VM queued bytes at end | 0 | 0 |
| Accepted points after pause | 253.17M | 285.73M |
| Flushed points after pause | 118.22M | 268.19M |
| Unflushed live + detached points | 134.94M | 17.54M |
| Active points | 99.73M | 0 |
| Detached points | 35.22M | 17.54M |
| WAL | 11.28 GB | 3.64 GB |
| Paused, forced-GC heap | 11.03 GiB | 6.94 GiB |
| Steady allocated bytes / 30s | 31.21 GiB | 44.19 GiB |
| Steady sampled CPU in GC | 48.2% | 31.4% |
| Live parts at final capture | 42 | 355 |
| Historical minimum samples/host | 3 | 5 |

The queue still grows and accepted rate trails offered rate. Memory still reaches
the 16 GiB cgroup cap (309,640 limit hits, zero OOM), with sampled peak RSS 15.07 GB.
CPU throttling totals 0.287s. The shutdown watchdog still exits with code 1.
All benchmark containers are stopped; profiles, matching binaries and volumes remain.
No live cluster was changed.

## What improved, and what it costs

The old whole-head flat builder is absent from the new heap profile. Frozen samples
feed the common streaming encoder in bounded, locally sorted identity runs; each
run may produce one or more parts. Input sample buffers remain readable until the
complete generation is published and its WAL prefix can be checkpointed. The quarter
workspace target is distinct from resident head/identity memory, not a total RSS cap.

At 240s and 300s the pressure worker is building a head part rather than waiting in
`memlimit.Pool.Acquire`. The first completed maintenance duration is 42.38s versus
72.26s; two cycles finish by final capture, the second taking 127.00s. These durations
include multiple stages and waiting, not just encoding. Full-generation publication
still waits for all outputs. The paused heap contains outstanding work and is not
an idle/leak measurement.

The additional part count is a material tradeoff. The old capture has 42 live parts
and no running merge; the new one has 355, a running merge and 16 candidates. More
data has been flushed, and bounding high-cardinality identity workspace also seals
output before the raw-row cap. This raises catalog/read/compaction work. The small
paired fixtures' unchanged 1/1/4 part counts do not establish unchanged fragmentation
at 14.49M-series scale. Keep the head PR draft until this pressure is evaluated.

The steady allocation window now contains 11.20 GiB of flush allocation (25.4% of
its total), versus effectively none while the previous pressure flush waited.
RW-to-pdata and pdata-to-native account for 17.78 and 9.56 GiB respectively (61.9%
combined). Total allocated bytes therefore rise while more work completes and GC's
CPU share falls; do not interpret this as a per-row allocation regression estimate.
The microbenchmarks separately show 12/31/61% fewer allocated flush bytes for
1/8/128 points per series, with increased allocation object counts.

The largest retained terms are about 1.66 GiB head maps/buffers, 1.56 GiB series
index, 0.65 GiB file-read payloads, 0.54 GiB per-series watermark updates, and 0.51 GiB
postings. Streaming sidecars/encoded metadata also remain resident; the dominant
whole-head column duplicate is removed, not every input or output allocation.

WAL still serializes writes, but caches per-append head state and admits readers
during encoding from immutable identity views. Paired native append benchmarks show
27–38% lower time; this run cannot isolate its contribution. The 300s trace still
records 17.1s aggregate writer wait over about 3s across goroutines. Completed
filesystem syscall delay is 320/292ms in the two traces, including about 205/207ms
fsync. Neither aggregate waits nor censored syscall samples are CPU percentages.

## Query and correctness checks

Both servers contain all 10,000 hosts. Native has five historical samples per host
versus VM's eight in the 120s gate; minimum is six. Native dashboard latency is not
scored on this incomplete dataset. VM completes all 190 selected requests per mode
without errors/empty results. Random p50/p95 is 2.26/6.53ms; repeated/cached is
0.89/1.48ms. Random selected queries use nocache=1; storage/OS caches remain warm.
Both full 242-query otelbench dashboard modes complete, with 218 nonempty and 24
unavailable-collector queries per dashboard. Otelbench has no explicit nocache flag.

Before capture, the combined source passed the complete storage suite, targeted
engine/WAL/index/guard race tests and full lint. E2E=1 native API/race checks cover
the pooled consumer, exemplars, simultaneous ingestion/query/compaction, multi-chunk
publication and file reopen. A deterministic native-file regression fails on old
code and passes when a large ordered head flushes while a merge owns its 3/4 share.
The existing duplicate-query defect #572 is separately reproduced and gated; this
change preserves raw duplicate rows rather than changing query deduplication.

Attribute research and the direct-hash/lazy-materialization design are in PR #1445.
Neither is enabled in this benchmark. They address conversion work before the
existing canonical identity lookup; the current results do not predict their capacity.
