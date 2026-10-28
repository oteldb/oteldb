# Metric batch-size sweep

Increasing the 4,096-point cap is not a consistent end-to-end improvement in this fixture.
The production cap remains unchanged. Normal vmagent v1.141.0 remote-write blocks default to
10,000 samples; the current cap typically splits them into three engine appends. That default
was checked directly with the pinned binary's `-help` output.

A Go overlay changed only `metricAppendPoints` to 4,096, 16,384, or 65,536. No production source
was changed for the experiment. Each variant used the same compact-index/batching baseline,
native file backend and WAL, no maintenance during the timed loop, GOMAXPROCS=8 and Go limit
14GiB. Temporary directories were on the workspace NVMe. Six repetitions of 20 iterations
rotated variant order. One untimed request registered the series before measuring repeated
writes. Requests contain one metric per series, 128 metric names, and 10,000 or 65,536 points.

Two paths separate storage work from adapter conversion: native preconverts the internal metric
input before the timer; adapter includes pdata-to-internal conversion on every write. PRW decode
and PRW-to-pdata conversion are outside both timers. This fixture holds cardinality fixed, appends
repeated timestamps to growing head buffers, and does not predict high-cardinality live capacity.

| Request / path | 4k cap | 16k cap | 64k cap |
|---|---:|---:|---:|
| 10k points / native | 3.42 ms | 3.15 ms (-7.9%, p=.026) | 3.14 ms (not significant) |
| 10k points / adapter | 5.82 ms | 4.79 ms (not significant) | 5.97 ms (not significant) |
| 65,536 points / native | 22.80 ms | 22.46 ms (not significant) | 23.88 ms (+4.7%, p=.026) |
| 65,536 points / adapter | 34.81 ms | 35.59 ms (not significant) | 38.27 ms (+9.9%, p=.026) |

The adapter allocated about 49.3 MiB per 65,536-point request at 4k versus 54.6 MiB at 64k
(+10.8%, p=.002). Native allocation remained about 3.1 MiB for all three. Growing sample buffers
contribute to these numbers; they are not purely batching scratch. Increasing the cap also
lengthens each serialized append, which must be weighed against readers and competing writers.
No live throughput or query-latency improvement is claimed from this sweep.

The larger opportunity is reducing conversion/allocation and maintenance contention, rather than
assuming fewer already-large WAL operations will remove the remaining bottleneck. The adapter's
extra time/allocations are observed here; its live share requires profiling. The direct remote-write
proposal is tracked separately in [oteldb#1106](https://github.com/oteldb/oteldb/issues/1106).

Artifacts: `results/batchsize-validation/`: benchmark binaries, exact overlays/source variants,
rotating runner, raw benchmark output, and `benchstat.txt`. Benchmark source is
`integration/prome2e/write_pressure_batchsize_test.go`, relative to the application root.
