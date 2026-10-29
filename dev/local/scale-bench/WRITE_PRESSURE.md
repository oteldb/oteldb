# Write-pressure investigation

The main write-path amplification is loss of batching: one remote-write request becomes
one OTLP metric per time series, then one engine append and WAL write per metric.
Flush/merge serialization, metadata rewriting, and memory pressure compound that cost.
This investigation adds diagnostic tests/scripts; it does not change production ingestion.

## Live evidence

Fresh 10k-host fixture, 15s scrapes, the existing compact image, native file backend,
8-vCPU quota, 16-GiB memory cap, no swap. Only oteldb received writes; VictoriaMetrics
was removed from the stack. No PromQL queries ran during the load. Prometheus counters
and debug logs were enabled, with a 30s CPU profile and a 5s runtime trace after 180s.
Instrumentation adds overhead, so this is diagnostic evidence rather than a clean capacity score.

Over 300s, vmagent offered 290.16M points and storage completed about 75.00M accepted
points: approximately **967k offered / 250k accepted points/s**. The pending remote-write
queue reached **1.44 GB**. No scrape failures occurred.

- WAL append counter: **75.04M**, approximately one append per accepted point.
- Process `write` syscall count: approximately **75.00M** at the same sampling boundary.
- Mean CPU usage: about **2.39 cores**; the 30s profile used about **2.68 cores**, below
  the eight-vCPU quota. CPU throttling accumulated only about 0.037s.
- Cgroup full I/O pressure accumulated about **0.58s / 300s**. This does not indicate
  a device-stall-dominated run, although WAL writes still consume syscall/CPU time.
- The five-second trace contains **28.57 seconds of aggregate `Engine.AppendBatch`
  lock wait**, equivalent to about 5.7 goroutines blocked on average. This is waiter-side
  delay, not a measurement of one lock holder's duration or a percentage of request latency.

## 1. Per-series batches amplify locks and WAL operations

The path is concrete:

1. `prometheusremotewrite/prw_to_metrics.go:45`: `FromTimeSeries` appends a new OTLP
   metric for every input time series, even when names and descriptors repeat.
2. `signal/metric/metric.go:155`: `Project` emits once per metric.
3. `storage.go:475`: each emission calls `Engine.AppendBatch`.
4. `engine/engine.go:455`: `AppendBatch` holds the engine mutex through admission,
   WAL encoding/write, and applying the samples.
5. `wal/segment.go:391`: each framed batch calls the file writer. The usual live
   remote-write shape contains one sample per series, so these batches contain one point.

Afterward `Storage.pokeFlush` can also acquire the head read lock for every emission.

The diagnostic fixture confirms the effect while checking data equivalence:

| 1,024 points | Current shape WAL appends | Grouped WAL appends |
|---|---:|---:|
| One metric name | 1,024 | 1 |
| 128 metric names | 1,024 | 128 |

Both forms retained the same canonical series and replayed timestamps/values, and
produced the same number of WAL bytes. The homogeneous forms produced identical
WAL byte streams. Grouping reduces operation overhead; it does not reduce those bytes.

Six repetitions on the NVMe-backed workspace filesystem, `GOMAXPROCS=8`, 1,024 points
per operation, fixed 100 iterations, no concurrent maintenance:

| Durable-file fixture | Current | Grouped | Speedup |
|---|---:|---:|---:|
| One metric name | 1.273 ms/op | 0.355 ms/op | 3.59× |
| 128 metric names | 1.260 ms/op | 0.493 ms/op | 2.56× |

Benchstat: `p=0.002`, `n=6`. With WAL disabled, grouping also reduced cost, showing
that metric conversion/projection and lock/call overhead matter alongside syscalls.
These small-cardinality microbenchmarks do not predict full 10k-host throughput.

## 2. Maintenance outlasts its nominal cadence

Completed operations during the five-minute load:

- **14 flushes: 144.50s total**, averaging 10.32s; one recorded flush took **39.18s**.
- **9 merges: 116.50s total**, averaging 12.94s.
- The reported head age reached approximately **34s**, despite a configured 10s interval.

`Storage.maintainOneEngine` calls flush and then merge. Those operations share the
engine's flush/merge exclusion, so another maintenance flush waits for that work.
The size-pressure trigger cannot make a busy maintenance worker finish sooner.

There are also large critical sections: flush constructs `head.identitiesOf` under
`Engine.mu`; streaming merge gathers identity references under its read lock.
Saved goroutine stacks show appenders waiting for the engine lock while flush builds
the identity snapshot. The trace confirms contention but cannot assign all writer
wait to flush rather than competing writers.

In the steady CPU profile, cumulative samples included approximately 27% in append,
27% in flush, 9% in streaming merge, and 19% in background GC. After stopping vmagent,
maintenance accounted for about 79% of a 15s CPU profile; identity encoding alone
accounted for about 39%. Background work continues after incoming writes stop.

## 3. Metadata and WAL identity records add substantial write volume

The backend recorded **20.76 GB of object writes**. Complete WAL rotations alone
accounted for at least **17.88 GB** more; process write accounting recorded about
39.1 GB. In contrast, completed flush/merge byte counters summed to about **3.44 GB**.

Those counters use `Manifest.DiskBytes`, which explicitly includes columns and marks
but excludes engine sidecars. Identity, `sidx`, per-series watermarks, and aggregate
statistics are written afterward. In-progress outputs also contribute to backend
bytes before completion counters advance, so the difference is not an exact sidecar
write-amplification ratio. It does establish that column counters are not total I/O.

At high cardinality, these sidecars and their construction arrays are proportional to
distinct series. Short parts with very few samples per series make that expensive.
Merge sizing also uses the column/marks byte figure, so physical metadata overhead
needs explicit consideration in size accounting and compaction decisions.

The WAL repeats a full identity whenever a series starts a new head buffer after
flush. That is deliberate: checkpointing discards earlier WAL identity records.
Skipping these records without a replacement recovery guarantee would lose identities
on replay. Fsync-every-append is disabled here; trace fsyncs still occur at segment
rotation/sealing and durability boundaries.

The process also reached the 16-GiB cgroup boundary with substantial reclaim events,
without an observed OOM kill. Sample maps/buffers, watermark and merge-sidecar arrays,
and serialization temporaries remain important after compacting resident identities.

## First changes to try

1. Preserve bounded batches through conversion and storage append. Coalesce only
   compatible descriptors within resource/scope/tenant boundaries, preserving admission
   and partial-success behavior. Keep acknowledgement after the WAL write completes.
2. Shorten flush/merge identity-snapshot critical sections, and let pressure flushes
   proceed without waiting behind long compaction cycles where the commit rules permit.
3. Stream/compact metadata sidecars, avoid unpacking/repacking compact identities in
   encoders, and account for sidecars in physical part/write-amplification measurements.

Simply raising CPU quota or flush frequency does not address the measured batch
fragmentation; increasing flush frequency can repeat more identity/sidecar work.

## Evidence and reproduction

Artifacts: `results/write-20261027T184924Z/`, relative to this directory:
`steady.cpu.pb.gz`, `steady.trace`, extracted sync/syscall/scheduler profiles,
loaded/draining heaps and CPU profiles, metric snapshots, cgroup/process I/O samples,
logs, `equivalence.txt`, and `batch-shapes.benchstat.txt`.

The reported benchmark uses `batch-shapes-disk.bench.txt`; the earlier
`batch-shapes.bench.txt` used the runner's default temporary filesystem and is not
the source of the reported disk numbers.

Diagnostic code: `write_pressure.py` and, relative to the application root,
`integration/prome2e/write_pressure_test.go`. The equality tests passed. Database
implementation and normal deployment configuration are unchanged by this investigation.
Diagnostic containers are stopped; this run's volumes were retained with `--keep`.
