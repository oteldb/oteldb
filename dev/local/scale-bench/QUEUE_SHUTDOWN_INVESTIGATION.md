# Queue growth and shutdown watchdog

The remaining queue is a capacity deficit, compounded by large resident heads and monolithic
metadata work. The watchdog is a separate shutdown problem: Close waits for background workers,
including long encoding/rollback phases, and the SDK exits before storage's own close budget.
One cancellation handoff bug found during this investigation is fixed in storage#793. The broader
capacity and shutdown-policy limits remain open.

## Queue: where the work and memory go

The corrected five-minute 10k-host run offered 290.17M samples and accepted 215.05M. The difference
is about 75M samples, consistent with the 496.85MB compressed remote-write queue. Admission
averaged ~717k/s against ~967k/s offered, so the queue grows by approximately 250k samples/s.
That result does not establish sustainable 10k-host capacity.

Batching already averages ~3,198 samples per WAL append (67,237 appends for 215.05M accepted).
The old one-WAL-call-per-sample amplification is removed. Six completed flushes covered only
129.19M samples; about 85.87M accepted samples were not yet covered by a completed flush at the
sampling boundary, including active/frozen and in-progress output state.

At the end of load, the pressure worker is waiting in `memlimit.Pool.Acquire -> reserveFlush`.
The main maintenance worker is actively encoding a merge's identity sidecar for about 10.1M
series in `identity.EncodeSorted -> View.At`. This is a long operation holding a build reservation,
not evidence of a pool deadlock. The quarter-budget flush reserve helps small flushes; an oversized
flush requests the whole pool and waits for the current merge to release it.

The sampled CPU profile uses ~4.51 cores, below the eight-core quota. Background GC is 31.4% and
allocation assists 9.0% cumulatively. `AppendBatch` is 20.1%, metric projection 19.1%, pdata conversion
8.0%, and flush-column construction 8.4%; these are overlapping cumulative stacks, not percentages
to add. The 180s heap attributes approximately 3.22GiB to head buffer/map allocation, 1.31GiB to
sample arrays, 1.29GiB to flush columns, 1.54GiB to the compact registry, and 0.58GiB to watermarks.
These are sampled allocation origins, not a proof that every byte is permanently retained.

A 64-bit `sampleBuf` contains three slice headers (72 bytes before allocator rounding). A fully
populated 14.49M-series head alone needs about 1GiB of those headers, before its hash map and sample
arrays. An active and a frozen generation can coexist. Compressing resident identity attributes
does not remove that per-series buffering cost or the flush's copied column arrays.

The identity encoder builds a full `body` slice, then appends that section into its final `dst`.
That retains large temporary representations and does serial per-series work. At high cardinality,
this work competes with ongoing ingest and GC. Cgroup full I/O pressure totaled ~0.48s in 300s;
combined with the CPU/stack evidence, this points to CPU/allocation/metadata coordination rather
than a demonstrated saturated storage device. Memory still reaches the 16GiB cap.

## Shutdown: observed sequence

The ordinary profiler endpoint closes on SIGTERM, so HTTP stack probes failed immediately.
`build_shutdown_probe.py` uses a temporary Go overlay to add a signal-triggered stack dumper to a
separate diagnostic image. Production Go source and shutdown policy are unchanged. It dumps after
1, 3, 7 and 11 seconds; raw logs and image metadata remain in the local run directories.

Before the cancellation guards, the 10k/180s probe shows:

| After SIGTERM | Close | Main maintenance | Pressure worker |
|---|---|---|---|
| 1s | WaitGroup.Wait | identity encoding | queued on flush mutex |
| 3s / 7s | WaitGroup.Wait | reattaching an aborted frozen head | queued on flush mutex |
| 11s | WaitGroup.Wait | starts a merge, then waits | starts a new large estimate scan |

Close had not reached its final synchronous engine flush. It cancels maintenance and waits on
`s.wg`, but workers ignored cancellation at queued ownership/stage boundaries. Tests reproduce
both engines flushing the new head after a queued call is canceled, and the facade starting a
merge after its flush canceled the maintenance context.

Storage commit `8769069` fixes those boundaries: check before/after flush ownership, keep canceled
empty-head flushes as no-ops, stop the pressure loop on cancellation, and stop starting further
maintenance stages. Head data stays queryable for a later flush. Gated tests fail before the fix;
affected suites, race checks and full lint pass afterward.

A second 10k/180s probe with the fix no longer shows the queued pressure worker restarting work.
It still shows already-running rollback at 1–3s, a merge encoding identities at 7s, and writing the
identity object at 11s. Close remains in WaitGroup.Wait. The identity encode loop and local file
write do not cooperatively observe cancellation inside those phases. The watchdog still exits 1;
Docker reports `OOMKilled=false` in both shutdown probes.

There is also an explicit timeout mismatch: `cmd/oteldb/app.go` gives `closeStore` a 30s context.
The pinned SDK v0.39.1 waits 5s before base-context cancellation and another 10s before `os.Exit(1)`,
so it kills the process at roughly 15s from SIGTERM. Its options provide no shutdown-duration
control. The storage close context cannot extend that outer limit, nor can it interrupt CPU loops
which do not check it. Simply increasing a timeout would not fix the continuing-work behavior.

## Follow-up boundaries

The cancellation handoff is fixed and published. The next focused implementation work is:

1. Make long identity/merge encoding phases cancellation-aware before publication, and stream
   bounded identity output instead of building/copying whole objects. Preserve completed CAS
   installation/checkpoint ordering when cancellation arrives after durable success.
2. Bound active/frozen head footprint including metadata and array capacity; reduce per-series
   pointer objects/duplicate lookup maps and conversion allocations. Preserve retryable
   backpressure rather than acknowledging and silently dropping samples.
3. Choose an explicit shutdown policy: allow enough time for part completion, or provide a
   deliberate durable-WAL close/replay path. Align application and SDK deadlines with that policy.
   This investigation does not silently change Close's flush contract or skip accepted data.

## Evidence and reproduction

The primary load is `results/write-20261028T171719Z/`. Shutdown probes before/after the handoff fix
are `write-20261028T184351Z/` and `write-20261028T190736Z/`. The ordinary-profiler failure is retained
in `write-20261028T183312Z/`. Curated normalized stacks and regression output are committed under
`results/shutdown-investigation/`; full profiles, logs, binaries and retained volumes stay local.

```sh
python3 dev/local/scale-bench/build_shutdown_probe.py
python3 dev/local/scale-bench/write_pressure.py --image local/oteldb:shutdown-probe --nodes 10000 --seconds 180 --keep
```

The diagnostic image's extra signal listener and stack allocations affect timing slightly; the
stacks identify control flow, not a clean capacity score. The original five-minute run uses the
normal image. All benchmark containers are stopped after collection.
