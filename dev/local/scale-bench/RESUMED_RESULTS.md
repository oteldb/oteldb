# Published-storage rerun, 2026-10-31

Application `9cd2b49b`, storage `v0.50.0` (`00569274`). Fresh image
`local/oteldb:node-resume-9cd2b49b`; exact image IDs are in each run's `images.json`.
Each database: 8 CPU quota, 16 GiB, no swap, `GOMEMLIMIT=14GiB`, native file/WAL,
10s flush. Fresh volumes per scale; vmagent separately gets 8 CPU / 12 GiB.
Self-metrics are enabled. These short shared-host trials do not establish
long-retention capacity or the isolated effect of any individual optimization.

| Hosts | oteldb | VictoriaMetrics | Sampled peak working set GiB, oteldb / VM |
|---:|---|---|---:|
| 1,000 | Freshness and clean restart passed | Freshness passed | 3.01 / 0.98 |
| 10,000 | Freshness failed; OOM during subsequent queue drain | Freshness passed | 15.12 / 9.77 |
| 25,000 | OOM at about 170s; last count 21,359 hosts | All hosts present; freshness failed | 15.77 / 14.38 |

## Queries

At 1k, all 19 selected queries returned data without errors in both engines.
Ten trials/query, four clients; pooled times are milliseconds:

| Mode | oteldb p50 / p95 | VictoriaMetrics p50 / p95 |
|---|---:|---:|
| Random hosts; VM response cache bypassed | 1.90 / 23.47 | 1.37 / 2.77 |
| Fixed host/timestamps; two warmups | 1.55 / 5.21 | 0.87 / 1.19 |

Both engines completed otelbench's 242-query dashboard at 1k in both modes.
Each had 218 nonempty targets and 24 empty targets, consistent with absent collectors.
Random mode executed 2,420 target/host pairs; repeated mode executed each target ten
times after two warmups. Warm storage/OS caches remain possible in random mode.

VM's 10k p50/p95 was 2.67/7.75 ms random and 1.02/1.92 ms repeated; its full dashboard
also completed. Oteldb gets no live 10k latency score after failing freshness.
Neither engine gets a 25k latency score.

The additional stopped-ingest 1k interleaved keepalive comparison retained CPU/heap
profiles. Oteldb medians: 1.524 ms random, 1.126 ms repeated, 1.124 ms repeated with
VM's response cache disabled. Postings seeking/intersection dominate query CPU;
`densePostings.Seek` accounts for 37.1% cumulatively in that profile.

## Write pressure

The final approximately 85s gate window at 1k offered 95,981 points/s; oteldb accepted
96,057 points/s. Snapshot timing/batching explains the small difference; both queues
were empty. At 10k, the corresponding window offered 966,239 points/s versus 729,424
accepted. Oteldb's queue grew from 585 MB to 709 MB and reached 798 MB after VM queries.
The historical sample minimum ended at two for oteldb versus eight for VM; scrape and
sample-drop counters were zero.

New scraping was then paused and the persistent queue resumed delivery. Oteldb was
OOM-killed at `00:51:06Z`, about 450s after startup and 50s into this drain. Its initial
six-minute live phase had no process exit. The drained query comparison could not run.
The early ingest heap retained 8.40 GiB, dominated by heads, series index, sample buffers,
and identity/flush scratch. The 30s ingest CPU profile is dominated by GC scanning.

At 25k VM's historical sample minimum ended at two. Both 2 GB remote-write queues
overflowed: persistent-queue counters recorded 3,930,125,106 bytes evicted for oteldb
and 133,119,199 bytes for VM. `vmagent_remotewrite_samples_dropped_total` stayed zero;
that counter alone misses persistent-queue eviction. Neither engine demonstrated
sustainable 25k throughput.

All benchmark containers are stopped; data volumes and raw profiles remain local.
Runs: `20261031T003917Z` (1k), `20261031T004336Z` (10k), `20261031T005155Z` (25k).
Selected reports/summaries are checked in under `results/`.

Validation: native file/recovery/publication E2E cases passed with `E2E=1`; Python
drain success/failure checks passed. Application lint passed, excluding ignored local
Go overlays under this benchmark's results/experiments folders.
