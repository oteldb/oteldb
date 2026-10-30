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
