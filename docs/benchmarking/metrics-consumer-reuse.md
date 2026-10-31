# Metric consumer descriptor reuse

`Backend.ConsumeMetrics` borrows `metric.Metrics` from the existing storage pool and
returns it after synchronous `WriteMetrics` completes. The converter still copies
identity payloads out of pdata. Only the nested resource/scope/metric/point descriptors
are reused. The prior fresh-batch comment assumed that retaining series bytes prevented
descriptor reuse; storage interns owned identities and copies samples before returning.
Exemplar ingestion also finishes before that return. Cluster framing owns its payloads
before routing completes. This does not authorize recycling the identity payload bytes.

The saved 10k-host profile put 76.6% of allocated bytes in the two conversion stages.
About 10 GiB/30s was attributed to metric/point descriptor growth in pdata-to-native.
This change addresses that growth without changing receiver routing or WAL behavior.

## Measurement

`BenchmarkConsumeMetrics` uses a fixed 10,000-series input, one point per series,
either one metric containing all points or 10,000 one-point metrics. Each point has
`instance` and `mode` labels. Each sample uses a fresh file backend and WAL, disabled
maintenance, one warm-up request, then 20 timed repeated requests. Storage is v0.50.0,
with its default WALSyncNone policy and 4,096-point append limit. Pdata construction
is excluded. The native control converts once before timing; the adapter converts
on every write. Sample buffers grow during timing; this is not a fixed-memory test.

Six samples before and after, GOMAXPROCS=8, Linux/amd64, AMD Ryzen 9 9950X3D2,
Go 1.27.3 (`nodwarf5`):

| 10k-point request | Before B/op | After B/op | Change | Median time before / after |
|---|---:|---:|---:|---:|
| Grouped adapter | 5.812 MiB | 2.441 MiB | -57.99% | 3.716 / 3.510 ms |
| Fragmented adapter | 7.023 MiB | 2.657 MiB | -62.16% | 5.280 / 5.044 ms |
| Grouped native control | 485.9 KiB | 485.9 KiB | unchanged | 1.915 / 1.970 ms |
| Fragmented native control | 485.9 KiB | 485.9 KiB | unchanged | 3.348 / 3.377 ms |

Allocation-byte reductions have p=0.002. No latency difference is statistically
significant (adapter p=0.065 grouped, p=0.485 fragmented). Fragmented allocation count
falls from 75.06k to 65.03k/request (-13.37%, p=0.002); most attribute allocations remain.
No live 10k capacity or GC improvement is established by this microbenchmark.

```
GOMAXPROCS=8 go test ./internal/storagebackend -run '^$' \
  -bench '^BenchmarkConsumeMetrics$' -benchtime=20x -count=6 -cpu=8
```

`sync.Pool` is a reuse hint, not a memory limit. A GC may discard entries, and an
oversized request followed by smaller requests can retain unused nested descriptor
tails and their attribute payloads. This change does not clear those tails or bound
the existing storage pool. An isolated conversion-only diagnostic (automatic GC
disabled during construction) builds 65,536 one-point metrics, then converts 100
one-point requests. After one forced GC, the pooled version retains 25,594,832 bytes
and 359,965 objects above baseline; fresh batches retain approximately zero. A second
GC with no intervening reuse releases the pool entry. This is a material tradeoff;
a hot pool entry need not become idle long enough to be discarded. It is not a
measurement of the live server heap. Concurrent production ingestion still needs
measurement before claiming a capacity benefit.

## Correctness

`TestStorageMetricsBatchReuse` exercises the consumer and deterministic reset/rebuild
of the same native descriptor batch, with changed identities, resources, scopes and
exemplars, and a large-to-small-to-empty shape. It queries both old and new values and
exemplars through the Prometheus API before flush, after flush and after file reopen.
The source pdata is cleared after each write. Race checks include the existing mixed
ingest/query/compaction regression; native Prometheus API, exemplar parity and dropped
record accounting checks also pass.

For the larger storage changes, see the [WAL and head design](wal-and-head-design.md).
