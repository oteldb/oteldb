# `count({...})` correctness bug — instant count-pushdown returns empty

## Symptom

`count({__name__=~"node_.+"})` (the suite's `worst case full series count`) and
every other **ungrouped instant** `count(<selector>)` returns **the empty
vector** on the `ghcr.io/oteldb/oteldb:v0.43.0` release image — confirmed live
on the benchmark stack:

```
count({__name__=~"node_.+"})            → status=success  result=[]          ✗
count(node_load1)                       → status=success  result=[]          ✗
count(up)                               → status=success  result=[]          ✗
count({job="node_exporter"})            → status=success  result=[]          ✗
{__name__=~"node_.+"}   (raw selector)  → status=success  result[142800]     ✓
count(node_load1) by (instance)         → status=success  result[100]        ✓
sum(node_load1)                         → status=success  result[1]={199}    ✓
count_over_time(node_load1[1m])         → status=success  result[100]        ✓
count(count(node_cpu_seconds_total) by (cpu)) → status=success result[1]={32} ✓
```

So the bug is narrow and specific: **ungrouped `count(<selector>)`**, exactly
the shape the count-pushdown optimizes. Everything that takes a different code
path (raw selection, grouped count, `sum`, `*_over_time`, nested `count by`)
is correct.

## Root cause: lookback-window clamp in the count pushdown

`count({...})` is routed to `countSelector` (`promql-engine` pushdown,
`execution/aggregate/count_selector.go`) instead of the materialize-then-count
path. Its `Next` computes the per-step count window and clamps it:

```go
// execution/aggregate/count_selector.go:99
start := o.currentStep - o.lookback
if start < o.mint {
    start = o.mint          // <-- BUG for instant queries
}
count, err := o.counter.CountSeries(ctx, start, o.currentStep, o.matcher...)
```

For an **instant** query the engine sets `mint == maxt == T` (the evaluation
time), so `o.currentStep == o.mint == T`. The intended lookback window
`[T − lookback, T]` is collapsed by the clamp to `[T, T]` — a single
millisecond. `CountSeries` then only counts series with a sample at exactly
that millisecond; with a 2 s scrape interval that is effectively zero series,
so `count` returns 0.

This is why the raw selector works but `count(...)` doesn't: the raw
instant selector gets its lookback applied by the storage's `Select` (the
normal path), whereas the count pushdown is responsible for its own lookback
and gets it wrong.

### Proof: range vs instant

The clamp only bites when `mint == currentStep` (instant). For a **range**
query `mint` is the range start, so `currentStep − lookback` is not below
`mint` for any step after the first, and the window is preserved. Verified
live on **both** the v0.43.0 release image and a local-HEAD build:

```
instant  count(node_load1)               → []          ✗
range    count(node_load1) [5m, step 30] → [{100 ×5}]  ✓
instant  count({__name__=~"node_.+"})     → []              ✗
range    count({__name__=~"node_.+"})     → [{142800 ×5}]   ✓
```

Same operator, same data — only the window math differs. That isolates the
bug to the clamp above.

## Why v0.43.0 shows *empty* instead of `{0}`

A second change layers on top. `countSelector.Next` historically always
emitted a sample for the single output series (the empty label set); commit
`1b87f03` ("execution: count pushdown emits no sample for an empty selector")
added a guard so a step with `count == 0` emits nothing (Prometheus semantics
for `count()` *over a genuinely empty input*). The release image (promql-engine
`v0.6.0`) carries that guard, so the clamp-induced `count == 0` is rendered as
the **empty vector** — silently wrong, no `{0}` tell-tale.

`7a42b7a` (local HEAD, "Revert …") removes the guard, so a fixed-clamp build
would emit the correct value; on the *unfixed* clamp it would surface the bug
as `{0}` instead of hiding it. Neither commit fixes the clamp — the root cause
predates both.

(Note: `24ad832` in `storage` — "fix count pushdown over-counting non-index-
safe matchers" — is a *different* count-pushdown correctness fix, for `!=`/`!~`
matchers over-counting. It does not touch this bug; `{__name__=~"node_.+"}` is
fully index-safe and always took the fast `counter.Count` path.)

## Impact on the benchmark numbers

`worst case full series count` reports p50 ≈ 317–320 ms in both `embedded-bench`
and the benchmark's REPORT — but that number is the cost of producing a
**wrong (empty) answer**. The pushdown still resolves all 142800 matching
series, decodes the parts (the decode-cache thrash from `FINDINGS.md`), and
binary-searches each series' timestamp run for a sample in the collapsed
`[T,T]` window before returning 0. So the latency is real work; the result is
garbage. `otelbench`/`benchctl` both run with `--allow-empty` / treat HTTP 200
as success, so the empty vector is counted as a passing run and the
~320 ms surfaces in the report as if it were correct.

## Fix

Remove the clamp — the lookback window for `count(selector)` at step `T` is
`[T − lookback, T]` whether or not `T` is the range start, exactly matching
Prometheus instant-vector semantics (a series counts if its latest sample is
within the lookback of `T`). One-line change in
`execution/aggregate/count_selector.go`:

```go
start := o.currentStep - o.lookback
// no clamp: the lookback legitimately extends before mint for the first
// step(s) of a range query and for every step of an instant query.
count, err := o.counter.CountSeries(ctx, start, o.currentStep, o.matcher...)
```

The guard removed by `7a42b7a` should stay removed (it only existed to mask
this bug): `count()` over a *genuinely* empty selector is already the empty
vector via the materialize-then-count path, but the pushdown's single
empty-label output series is correct when `count > 0`; emitting `{0}` for a
truly-empty window would diverge from Prometheus, so keep the revert and fix
the clamp.

### Regression coverage

The pushdown has no test that exercises an instant query against a series
whose samples are offset from the evaluation timestamp (the configuration that
triggers the clamp). Add one: ingest a series at scrape times `T-4s, T-2s`,
evaluate `count(metric)` at `T` (no sample at `T`), assert `{2}` (or whatever
the lookback covers). Both the instant and range forms should agree. Until
that exists the pushdown will regress again — the two promql-engine commits
above show it has already broken twice.

## Status (fix applied)

The clamp is removed and a focused regression test added —
`promql-engine/execution/aggregate/count_selector_test.go` — with three cases:
instant lookback (`TestCountSelectorInstantLookback`), range per-step window
(`TestCountSelectorRangeLookbackWindow`), and empty-selector emit
(`TestCountSelectorEmptySelectorEmits`). The instant test fails on the clamped
code (window `[T, T]`) and passes with the fix (window `[T − lookback, T]`).

Verified end-to-end against a live `embedded-bench` stack (oteldb built from
the local `replace`d `promql-engine`/`storage`), 100 nodes, ~140k series:

| query | before (v0.43.0 + HEAD) | after fix |
|---|---|---|
| `count(node_load1)` instant | `[]` | `{100}` |
| `count(up)` instant | `[]` | `{100}` |
| `count({__name__=~"node_.+"})` instant | `[]` | `{142800}` |

To iterate on storage/engine changes against this bench, the oteldb `go.mod`
needs local `replace` directives — without them the bench builds against the
*published* `promql-engine@v0.6.0` and local edits never reach the binary:

```
replace github.com/oteldb/storage => /src/oteldb/storage
replace github.com/oteldb/promql-engine => /src/oteldb/promql-engine
```

These are iteration-only (developer host paths) and must not be committed.

The bench harness now also self-checks: `main.go` runs a `verify` pass before
timing (one request per query, asserting non-empty results and positive
`count` values) and passes `--allow-empty=false` to otelbench, so a future
regression of this shape fails the bench loudly instead of reporting a faster
latency for a wrong answer.