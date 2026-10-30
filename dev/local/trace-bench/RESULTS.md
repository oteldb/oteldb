# Trace-only file-storage baseline

Run `20261030T213523Z`: fresh oteldb image, application `fcd1ccd`, storage `fbcd6b8`
([lineage PR #797](https://github.com/oteldb/storage/pull/797)). Linux/amd64,
Ryzen 9 9950X3D2, Go 1.27.3. CPU is a quota on a shared host.

| Configuration | Value |
|---|---:|
| Server quota | 8 vCPU, 16 GiB RAM, no swap |
| Go limits | GOMAXPROCS=8, GOMEMLIMIT=14GiB |
| Storage | Native file backend and WAL, 10s flush |
| Requested traffic | 1,000,000 uncompressed protobuf bytes/s |
| Duration | 300.2s ingestion, then 45s drain |
| Shape | 16 stable streams, 8 attributes/span, 512-byte unique hex attribute |

IDs are deterministic synthetic 16-byte trace and 8-byte span IDs. Four spans
form one trace. No metrics, logs or exemplars are ingested; self metrics are
scraped out of process. No S3 or ClickHouse is involved.

| Measurement | Result |
|---|---:|
| Actual payload rate | 999,999 bytes/s |
| Accepted payload | 300,196,800 bytes |
| Accepted spans | 345,600 (~1,151/s) |
| Rejected / failed spans | 0 / 0 |
| Baseline process RSS | 64.5 MiB |
| Peak sampled process RSS | 312.2 MiB |
| Peak container memory, including file cache | 483.3 MiB |
| Steady heap profile, at ~120s | 79.5 MiB in use |
| Heap profile immediately after ingestion and forced GC | 51.4 MiB in use |
| Heap profile after draining and forced GC | 57.8 MiB in use |
| Process RSS after drain, before final forced GC | 252.3 MiB |
| Peak sampled trace head | 9.74 MB |
| Trace identity state | 24,849 bytes, 16 streams |
| Fresh stored flush bytes | 271.6 MB |
| Merge output bytes | 1.558 GB |
| Merge rewrite factor | 5.74x |
| Total allocated bytes, including garbage collected | 31.2 GB (~104x payload) |

RSS and heap profiles have different meanings: the process retains address-space
pages and pools after garbage is collected. Peak RSS is sampled every five seconds;
container peak includes page cache. Heap profiles use Go's allocation sampling.
The ~104x number is cumulative allocation divided by received protobuf bytes,
not resident memory amplification. The baseline is one bounded run, not an A/B
optimization result or proof of long-retention sustainable capacity.

After draining there is one readable part, no buffered head, no WAL bytes and no
merge candidates. The trace catalog has zero claims and gaps. Storage shuts down
with exit code 0; no OOM occurred. The benchmark's container and volume are retained.

## Interpretation and next measurement

This trace shape does not reproduce the screenshot's estimated ~560 MB of trace
RAM or its 51x one-hour rewrite factor. The screenshot apportions shared heap by
merge traffic; it does not measure retained trace heap directly. An older backlog,
a different span shape, identity cardinality or a longer run may behave differently.

The dominant measured cost is allocation churn. Final allocation profiling assigns
25.9% flat to Zstd `encodeAll`, 14.4% to compression output reservation and 13.7% to
attribute `KeyValue` slice growth. These are independent flat allocation samples.
The post-drain heap is dominated by Zstd history/encoder workspace, rather than
lineage or stream identities. Full profiles remain with the local raw run.

The next optimization experiment should measure compression/attribute buffer reuse
at this same 1 MB/s rate and shape, checking both allocation rate and retained heap.
Retaining larger pools can reduce churn while increasing steady RAM. Longer runs
and higher-cardinality or older timestamps are separate controls for rewrite growth.

See [selected counters](results/20261030T213523Z/selected-results.json),
[steady heap](results/20261030T213523Z/steady.heap.top.txt),
[final allocations](results/20261030T213523Z/final.alloc.top.txt), and
[post-drain heap](results/20261030T213523Z/drained-gc.heap.bytes.txt).

## Allocation and merge follow-up

Runtime changes are recorded in storage commits `920a3b3` (buffer reuse) and
`6ac0c29` (compression fallback). Images were built from isolated worktrees before
committing: their labels record the parent revision plus source-diff hash, and each
raw run retains the exact server executable for profiling. All four runs use the
same 1 MB/s protobuf rate, 16 streams, payload shape, 10s flush, 8 vCPU/16 GiB and
fresh independent file/WAL volumes. Each accepted 345,600 spans with zero failures
or rejections and exited 0 without OOM.

| Variant | Allocated bytes | Rewrite factor | Sampled peak RSS | Merge seconds | Final parts |
|---|---:|---:|---:|---:|---:|
| Original | 31.20 GB | 5.735x | 312 MiB | 42.79s | 1 |
| Reuse frame + attribute buffers | 18.45 GB | 5.734x | 319 MiB | 41.06s | 1 |
| Reuse + ordinary growth threshold 3x (diagnostic) | 18.52 GB | 5.732x | 318 MiB | 42.36s | 1 |
| Reuse + compression fallback fix | 11.13 GB | 1.471x | 329 MiB | 13.00s | 3 |

These are single five-minute runs per image, followed by 45s drain. They are a
controlled diagnostic comparison, not a statistical end-to-end throughput result.
Peak RSS did not fall: allocation reuse and less I/O do not imply a smaller
instantaneous working set. Forced-GC heap snapshots also depend on pool/maintenance
timing and must not be treated as a stable capacity estimate.

### Allocation changes

Streamed frames reuse their encoded destination only after the sink's `Write`
returns; buffered frames retain independent ownership. Attribute-key enumeration
reuses and clears decode scratch. Both capacities are charged to writer memory.
Six-run microbenchmarks show Zstd frame allocations 2823.9 -> 190.5 KiB/op
(-93.25%, p=0.002) and flat attribute-key enumeration 704 -> 0 B/row after warmup
(p=0.002). Corresponding times fall 30.72% and 30.13%.

### Why changing normal merge size alone did not help

The [diagnostic patch](experiments/merge-growth3.patch) raises only the ordinary
run's growth target from 1.7x to 3x, retaining disk sealing and idle fallback.
It still performs 31 merge/upgrade operations and rewrites ~5.73x, because the
compression-only fallback bypasses the ordinary guard: a fresh part needing Zstd
compression fills its remaining budget with already-compressed neighbors.

The fix restricts compression-only work to parts requiring an upgrade. Balanced
ordinary compaction remains responsible for combining cold neighbors, and explicit
force can still collapse an uneven pair. A memory/file regression fails before
this change and passes after it, preserving row contents across durable reopen.

Combined allocations fall 64% and merge output falls 74% versus original. There
are 40 total merge/upgrade operations versus 31: the useful change is fewer costly
old-data rewrites, not fewer operations of every kind. After draining, three readable
parts remain, head and WAL are empty, and no merge is running. This trades more
remaining parts for less rewriting; query fan-out was not benchmarked here.

Full storage tests, focused race checks, attribute-scratch fuzzing and lint pass.
The 3x-growth diagnostic is excluded from the product changes. See the compact
[comparison](results/allocation-comparison.json) and per-run selected counters;
raw profiles, binaries and volumes remain local and are not committed.
