# Compact identities and selective index reads

Implemented and measured on 2026-10-27. Resident identity/index allocations fell about
88%, and median query latency improved about 3–4× on the fixed 1k-host dataset.
The implementation does **not** establish sustainable ingestion at 10k or 25k hosts:
both optimized runs stayed alive for six minutes but failed the freshness/history gate.

## Implementation

Storage worktree: `/home/tdakkota/workspace/github.com/oteldb/oteldb/.worktrees/compact-series-storage`,
branch `claude/compact-series-index`, based on `9765fda843d16b52d8bd40ff854e8e9412e11cc6`.
Application/benchmark worktree: this checkout, branch `claude/node-exporter-scale-benchmark`.

- Resident identities use **32-byte rows**, shared resource/scope contexts, shared ordered
  attribute-name schemas, and **4-byte typed-value references** in contiguous arenas.
- Metrics postings store **4-byte engine-local ordinals** and convert them back to canonical
  128-bit hashes before accessing parts. The all-series list avoids per-label duplication.
- Immutable compact views support off-lock pruning and metrics flush/merge snapshots. Metrics
  identity encoding materializes one series at a time; WAL encoding reuses attribute buffers.
- Sparse part intersections use binary searches; dense intersections retain linear scanning.
- Large series indexes have one fence per 1,024 entries and read **20-KiB pages** through a cache
  sharing the whole-object read budget. Mutations invalidate page generations coherently.
- Canonical hashes and existing disk/WAL formats are preserved. Full index validation still reads
  each sidecar once at open; full scans and metadata warming can still request a whole index.

The optimized image is `local/oteldb:scale-compact`, image manifest
`sha256:33080e83508de32753828f9436ee568229fa87a6a92960437cf579afa3113b61`.
The application base is `e5de6b841f45e167ec484be59c86cfecdfbbd7ef`; it was built with
Go 1.27.3 and a temporary modfile pointing at the storage worktree. The application's
pinned `go.mod` and the other storage checkout were preserved.

## Fixed-dataset comparison

Both versions opened identical copies of the retained **1,000-host / 1.449M-series**
file dataset. Ingestion was stopped. Requests used fixed timestamps, persistent HTTP
connections, and shuffled server order. There were five warmups and 30 measured trials
for each of the 19 dashboard queries in each mode: **570 measurements per server/mode**.
All **1,710 measured before/after pairs** had identical returned labels and sample values.

| Mode, pooled median over the query mix | Before | After |
|---|---:|---:|
| Random hosts | 4.99 ms | 1.65 ms |
| Repeated host, caches allowed | 4.52 ms | 1.14 ms |
| Repeated host, VM response cache disabled | 4.54 ms | 1.31 ms |

VictoriaMetrics remains faster: its corresponding medians were 0.57, 0.16, and 0.32 ms.
Changing hosts does not flush OS/index/column caches. The VM random and explicit
no-response-cache modes pass `nocache=1`; oteldb has no equivalent response cache here.

After explicit GC, Go `HeapAlloc` was **1.86 GiB before / 0.52 GiB after** (about 72%
lower). The dominant sampled identity/index allocation sites summed to approximately
**1.69 GiB before / 0.20 GiB after** (about 88% lower). These are heap observations,
not a guarantee about total process memory. Query profiles warmed different amounts
of decoded data, so identity/index allocation sites are the cleaner representation comparison.

## Identity registration microbenchmark

Six sequential repetitions per version, `GOMAXPROCS=8`, same host and corpus. These
counts are **series, not hosts**. Benchstat reports `p=0.002`, `n=6` for each comparison.

| Register a 10k-series corpus | Before | After | Change |
|---|---:|---:|---:|
| Time | 3.579 ms | 2.512 ms | −29.8% |
| Allocated bytes | 12.062 MiB | 2.708 MiB | −77.6% |
| Allocations | 10,568 | 624 | −94.1% |

The full 1k/10k-series benchstat output is in the evidence directory below.

## Live ingestion and query benchmark

Same nginx → vmagent fan-out, 15s scrape interval, native file backend, 10s flush
cadence. Each database had an **8-vCPU quota, 16-GiB hard memory cap, 14-GiB Go soft
limit, and no swap**. VM/vmagent v1.141.0; node_exporter v1.8.2. The servers shared
one host and disk; cores and I/O were not dedicated. Every scale started with fresh volumes.

At 1k, all 190 measured requests per mode returned nonempty results, with no errors.
The file-backend clean restart check also passed.

| Oteldb live 1k result | Before | After |
|---|---:|---:|
| Random-host p50 / p95 | 8.27 / 30.59 ms | 1.94 / 21.97 ms |
| Repeated-host p50 / p95 | 7.01 / 21.59 ms | 1.70 / 3.72 ms |
| Sampled peak working set | 5.98 GiB | 2.91 GiB |

Cold/random-query tails improved less than medians; the remaining index/postings and
column work is still material. This is a short live dataset, not a long-retention test.

| Hosts | Before | Optimized run |
|---:|---|---|
| 1,000 | Passed | Passed |
| 10,000 | OOM-killed after ~95s | No OOM observed in six minutes; reached all hosts at ~78s, but ingestion fell behind and the history gate failed |
| 25,000 | OOM-killed after ~114s | No OOM observed in six minutes; never passed the all-host/history gate |

The optimized 10k sampled working set peaked at **15.69 GiB**, with about **1.65 GB**
pending remote-write data by the end. Captured sample timestamps lagged wall time by
several minutes. At 25k, the largest observed recent-host count was 18,256; the final
count was 14,502 as older samples aged out of the query lookback. These are observations
of `node_uname_info`, not exact registration counts.

VictoriaMetrics passed at 10k, but also failed the history gate at 25k in this shared-host
run. Both 25k remote-write queues reached their configured ~2-GB bounds. No 10k/25k
oteldb query score is reported, and no 25k VM score is reported: incomplete/stale
datasets would make those latency numbers misleading. 50k/100k were not run.

## Remaining high-cardinality cost

The 10k ingestion heap is dominated by sample buffers and maps, compact identities,
streaming-merge sidecar arrays, watermarks, and encoded metadata. The former giant
per-series `Attributes.Intern` allocation is gone.

The saved 25k goroutine snapshot shows remote-write calls waiting in `Engine.AppendBatch`
for `Engine.mu`, while a flush constructs the identity snapshot under that lock.
The short ingestion CPU profile also shows map/identity work and maintenance. This
supports further work on flush planning and write concurrency; it does not establish
one exclusive cause of the entire throughput deficit. Compact identities alone do
not remove the sample-buffer, metadata-sidecar, or serialization costs.

## Validation and artifacts

- Complete storage `go test ./... -timeout 10m`: passed.
- Race detector over index, backend, metrics engine, and record engine: passed.
- Full storage lint: zero issues.
- Compact-identity coverage: **97.8%**. Fuzzing: ~669k compact-identity cases and ~330k
  dense-intersection cases; no failures.
- Native-file Prometheus HTTP E2E with `E2E=1`: repeated writes, two flushes, reopen,
  count/host/negative filters, rates, and label enumeration passed.
- The full 242-target node-exporter dashboard completed through the fresh otelbench
  against the optimized image and VM. Both returned the same 24 empty targets. The
  two sensor-name joins were scoped to the selected host, as in the earlier run.

Evidence, relative to this directory:

- `results/compact-20261027T142547Z/`: image/source provenance, build-time patch,
  before/after benchmark samples and benchstat, post-GC heaps/runtime stats, exact
  query equivalence, paired timings/profiles under `frozen-1k/`, and dashboard reports.
- `results/20261027T143241Z/`: live 1k/10k/25k reports, raw requests, readiness checks,
  memory/CPU samples, logs and queue counters; 10k/25k heap and lock snapshots.
- `integration/prome2e/storage_compact_test.go`: durable HTTP regression fixture,
  relative to the application root.

Benchmark containers are stopped. The frozen comparison datasets are retained;
the live-run temporary volumes were removed by the runner. The implementation is
local and uncommitted at measurement time. Review commits are now linked from CONCURRENCY_RESULTS.md.
