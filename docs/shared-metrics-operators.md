# Shared metrics operators

TraceQL metrics planning uses scarecrow's column operators for scalar arithmetic,
vector arithmetic, result filters, and top/bottom-k. It does not introduce another
numeric evaluator or persist query results. The existing PromQL planner uses the
same physical operators.

## Boundary

`traceqlengine.BuildMetrics` lowers an already parsed `traceql.MetricsExpr`.
A `MetricsSource` supplies a fresh operator for each first-stage aggregation.
It can read and aggregate spans directly, use a storage pushdown, or read a
compatible materialized metric. `MetricsCompareSource` is an optional capability
for selection/baseline aggregation; unsupported sources reject `compare`.

```text
TraceQL AST → source aggregation/buckets → scarecrow columns/operators → consumer
                  ↑
       projected spans, storage aggregates, or compatible persisted metrics
```

The source owns span selection, structural predicates, bucket boundaries, group
discovery, sampling, and first-stage aggregation. In particular, TraceQL `rate`
counts spans per second; PromQL's counter rate and distinct-series count are not
substitutes. Averages across partial aggregates must combine sums and counts.

First-stage schemas contain losslessly encoded group identities and remain stable
once returned. Prometheus string labels must not collapse typed TraceQL values,
such as integer `1` and string `"1"`, into the same identity. The source or response
adapter retains the typed label metadata and exemplars; numeric columns alone do
not carry them. Native [count/rate metrics queries](./traceql-metrics.md) supply
the concrete span source and Tempo response adapter. Other first-stage aggregates
remain separate capabilities.

## Runtime contract

- `NewScalarBinary`, `NewVectorBinary`, `NewTopK`, and `NewBottomK` construct
  physical operators without accepting PromQL AST nodes.
- `NewEvalContext` creates a borrowed step grid and a shared sample budget.
  Sources charge returned bucket values with `ChargeSamples` and separately
  enforce storage read/memory budgets. Reuse the budget across query chunks.
- `Prepare` resolves schemas before execution; ranking can consume input here.
  `BuildMetrics` calls it and closes acquired operators if planning fails.
- Successful constructors take ownership of their inputs; failed constructors
  leave ownership with their caller. Callers close a successfully built root,
  including after cancellation or an execution error.
- `Next` borrows one column until the next call. Columns are not retained by
  streaming arithmetic/filter operators; binary joins retain the build side and
  ranking retains its selected results. Shared operators preserve this existing
  memory behavior.
- Sources mark NaN gaps absent. Top/bottom-k chooses survivors independently at
  each step, preserving gaps for steps where a series was not selected.

## Language policies

The zero value of `scarecrow.BinaryOptions` retains PromQL behavior. TraceQL sets:

- `NaNOnZeroDivision`: a zero divisor produces a NaN/gap rather than infinity.
- `NaNIsAbsent`: NaN operands/results do not survive, including `!=` filters.
- `PreserveMetricNames`: scalar operations retain names; vector arithmetic
  combines both names when present, following Tempo.
- `BroadcastUnlabelled`: an ungrouped series can match grouped series. Exact
  group matches take precedence over the ungrouped fallback, and noncommutative
  operations preserve their original operand order.

Duration thresholds are converted to seconds by the TraceQL planner, matching
duration-valued first-stage output. The source's filtering timestamps still need
their original precision; the runtime grid stores output timestamps in milliseconds.

Compatibility references, pinned to Tempo 0b2b3c3:

- [Metrics arithmetic, broadcasting, and names](https://github.com/grafana/tempo/blob/0b2b3c361d38243367c368abe58642f88bdee7cf/pkg/traceql/ast_metrics_math.go).
- [Duration thresholds and result filters](https://github.com/grafana/tempo/blob/0b2b3c361d38243367c368abe58642f88bdee7cf/pkg/traceql/ast_metrics.go).

LogQL still uses its existing metric evaluator. Its parsing/extraction pipeline can
later feed the same column runtime after its window, label, and error semantics
have equivalent adapters and regression coverage.
