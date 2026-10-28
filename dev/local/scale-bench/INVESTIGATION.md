# Oteldb cardinality OOM and dashboard latency investigation

2026-10-27. Scaling stopped at 25k as requested; 50k and 100k were not started.
The 25k volumes, original 1k profiling fixture, experimental clone, logs, and profiles
are retained. Production source and the pinned storage dependency are unchanged.

## Setup and observed outcomes

Oteldb was freshly compiled from `e5de6b841f45e167ec484be59c86cfecdfbbd7ef` with
Go 1.27.3 and built into `local/oteldb:scale-e5de6b84` / `local/oteldb:scale-bench`.
Storage: `github.com/oteldb/storage v0.49.5-0.20261026132056-9765fda843d1`.
VictoriaMetrics/vmagent: v1.141.0. Exporter: v1.8.2. Nginx: 1.27-alpine.

Each server had an enforced 8-vCPU quota, 16-GiB cgroup memory cap, no swap, and
`GOMEMLIMIT=14GiB`. This was a shared Ryzen 9 9950X3D2 host, not dedicated cores.
Oteldb used its native `file` backend with a persistent WAL, compressed parts, and
10s flush cadence. No S3, ClickHouse, or ephemeral storage engine was used.

One real node_exporter → nginx with a 2s response cache → vmagent with distinct
`instance="host-N"` labels, 15s scrapes, and identical remote-write streams to both
servers. Each scale used fresh volumes. Cardinality was 1,449 series per host.

| Requested hosts | Oteldb | VictoriaMetrics |
|---:|---|---|
| 1,000 | All hosts; 1.449M series; 19-query suite passed | Same cardinality; suite passed |
| 10,000 | OOM kill after ~95s; last observed host count 6,083 | 14.49M series; suite passed |
| 25,000 | OOM kill after ~114s; last observed host count 6,441 | Reached 25k hosts / 36.225M series; stopped before the history/query gate |

Host counts above are observations of `node_uname_info`, not exact counts of fully
ingested hosts. Both OOMs have Docker `OOMKilled=true`, exit code 137. The 25k run
has no latency score because the user stopped it before history verification.

At 1k, the initial 19-query test made 190 measured requests per server/mode with
four concurrent clients, fixed timestamps, a 60s range / 15s step, and 1m rate
windows. There were zero errors or empty results. Oteldb also passed a clean
restart persistence check. VictoriaMetrics random requests used `nocache=1`;
repeated requests warmed host-0 twice and allowed its rollup cache.

| Server | Random p50 / p95, ms | Repeated p50 / p95, ms |
|---|---:|---:|
| Oteldb, 1k | 8.27 / 30.59 | 7.01 / 21.59 |
| VictoriaMetrics, 1k | 1.52 / 3.27 | 0.89 / 1.28 |
| VictoriaMetrics, 10k | 1.77 / 4.25 | 0.91 / 1.23 |

## OOM: resident identity state survives disk flushes

The 10k heap snapshot, taken ~39s before the OOM, already held ~8.75 GiB of sampled
live objects. The largest allocations were:

| Allocation | Live GiB | Share of sampled heap |
|---|---:|---:|
| `signal.Attributes.Intern` | 4.36 | 49.8% |
| `series.Index.Add`, excluding its children | 1.36 | 15.6% |
| Postings arrays and all-series list | 1.41 | 16.1% |

The code explains the retention:

- `index/series/series.go:70`: each distinct series retains an `Entry` and an owned
  point-attribute array. Resource/scope objects and string payloads are already
  interned; the per-series point-attribute structures still remain.
- `signal/attributes.go:441`: `Attributes.Intern` allocates a new `[]KeyValue`.
  Measured sizes on this build: `KeyValue=80 B`, `Series=168 B`, `Entry=184 B`.
- `engine/head.go:107`: identity state is explicitly separate from buffered sample
  bytes because a flush cannot drain it. Postings and per-series watermarks add
  more resident state.
- `engine/prune.go:14`: pruning removes identities whose retained data has died;
  it cannot evict the active series in this workload.
- `tenant/tenant.go`: cardinality and in-flight limits default to unlimited.
  This benchmark config supplies no `storage.policy.limits`; the query/decode
  budgets do not impose a resident identity budget.

On the 1k fixture, after flushing and restarting, the inspector reported 1.449M
series, **zero head samples/bytes**, and one disk part. Nevertheless, the four
identity/index allocation sites above retained about **1.74 GiB**. A simple linear
extrapolation gives **~17.4 GiB at 10k**, before caches, sample buffers, WAL,
flush/merge transients, and runtime overhead. This is an estimate, not a measured
full-10k heap. It explains why a shorter flush interval or Go's soft memory limit
cannot make the current representation fit 16 GiB.

The evidence supports cardinality-driven retained state, rather than duplicate
series creation on every scrape or a query-induced OOM. Both OOMs occurred during
ingestion, before the query benchmark.

## Latency: series discovery and index algorithms dominate

A fresh 1k fixture reproduced the original result. Then vmagent was stopped,
data flushed/restarted, and the same fixed dataset was queried through persistent
HTTP connections. Server order was shuffled within paired measurements. Each
query/mode had five warmups and 30 measured trials; all returned nonempty results.

| Query, median ms | Oteldb random | VM random, response cache off | Oteldb repeated, response cache off | VM repeated, response cache off |
|---|---:|---:|---:|---:|
| CPU Busy | 28.21 | 2.61 | 7.03 | 0.68 |
| CPU Basic, six mode exclusions | 16.75 | 1.29 | 15.79 | 0.72 |
| Raw total memory | 1.49 | 0.11 | 1.74 | 0.10 |

Thus HTTP connection setup, concurrent ingestion, and VictoriaMetrics response
cache hits do not account for the gap. OS, index, and decoded-column caches remain
warm; these are not cold-disk measurements.

In a 25s, four-client, random-host query-only CPU profile:

- **65.8%** of oteldb CPU samples passed through `scarecrowScanner.Series` /
  `Storage.MetricSeries` / `Engine.Series`: schema discovery before evaluation.
- **44.8%** of total CPU samples passed through `partIndex.intersectMark`.
  `engine/part.go:200` performs a linear merge of matching IDs against the part's
  full series index. With hashed IDs spread throughout a 1.449M-series part, a
  one-host query still walks most of that index, once per relevant selector.
- Postings intersections/seeks were another large cost, especially with negated
  mode matchers. `head.resolve` unions matching values with `WithoutName` for
  predicates matching absence, then intersects the resulting global postings.
  Several such predicates make repeated seeks across large lists expensive.

The native server processed 18,264 requests in that profile versus 212,660 for
VictoriaMetrics under the same client shape with its response cache disabled.
These are diagnostic profile counts, not standalone throughput capacity scores.

### Controlled sparse-lookup experiment

Only a **copy** of the pinned module was changed, under `experiments/storage`.
For sparse matches (`matches * 64 < part series`), `intersectMark` used the existing
binary-search `has` lookup. Dense intersections retained the original algorithm.
The experimental image ran on a clone of the stopped 1k volume, under the same
limits. The original dataset and source were preserved.

| Pooled median over the same 19-query mix | Original, ms | Sparse lookup, ms |
|---|---:|---:|
| Random hosts | 4.68 | 1.74 |
| Repeated host, cache allowed | 4.67 | 1.22 |
| Repeated host, VM response cache disabled | 4.58 | 1.21 |

The part-wide scan disappeared from the top profile. The remaining dominant path
was `head.resolve` / postings intersections (~70% of sampled CPU). The experimental
server processed 34,924 profile requests in 25s. Exact returned labels and sample
values matched the original for **76 query/host pairs**. Sparse/dense, resident/paged,
missing-ID, and pre-existing-match tests passed, along with existing series/count tests.

To isolate the negated-mode cost, the six-exclusion CPU query was also expressed
as the equivalent positive mode set **for this captured exporter dataset**:

| Variant | Exclusions, median ms | Positive mode set, median ms |
|---|---:|---:|
| Unchanged oteldb | 15.84 | 5.91 |
| Sparse-lookup experiment | 14.08 | 2.06 |

Returned values matched in both experiments. This is diagnostic evidence, not a
general dashboard rewrite: a fixed positive set would omit newly introduced modes.

## Full dashboard and next changes

The fresh `otelbench dashboard bench` also completed all **242 visible targets**
on unchanged oteldb and VictoriaMetrics: three random-host variants, plus three
repeated trials after two warmups. Collapsed rows were flattened and the two bare
`node_hwmon_chip_names` joins were scoped to the selected host; otherwise cloned
chip names cause a many-to-many error. Each server returned the same 24 empty
targets, consistent with collectors absent from this fixture. These supplemental
runs apply a range to every target and do not force response-cache bypass; the
19-query measurements above provide the controlled random/cache comparison.

The next implementation work should address two separate problems:

1. Compact or page resident series identities and point-label structures, and
   account for identity growth in admission. A configured series ceiling can
   protect the process by rejecting data, but does not make all 10k hosts fit.
2. Make part intersections adapt to selectivity, then evaluate absent/negative
   predicates against a selective candidate set. Preserve absent-label and
   resource/scope/point-label semantics. The isolated sparse patch demonstrates
   part of the gain; it does not solve OOM or remove the remaining CPU cost.

## Evidence

- Initial scale data: `results/20261027T123702Z/1000/`.
- 10k OOM, heap, and VM benchmark: `results/20261027T124043Z/10000/`.
- 25k OOM and retained-container metadata: `results/20261027T124043Z/25000/`.
- Controlled fixture, original profiles, and paired timings:
  `results/20261027T124737Z/1000/diagnosis/`.
- A/B timings, profile, equivalence checks, and predicate comparison:
  `results/20261027T124737Z/1000/diagnosis-sparse/`.
- The 14-line diagnostic patch:
  `results/20261027T124737Z/1000/diagnosis/sparse-experiment.patch`.
- Full-dashboard YAML reports/logs: `results/20261027T124737Z/1000/otelbench-*`.

All paths are relative to this directory. Profiles are sampled evidence; short live
datasets do not establish long-retention steady-state capacity or latency.
