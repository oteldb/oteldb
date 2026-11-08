# TraceQL compatibility and Traces Drilldown

This audit compares oteldb with Tempo and Grafana Traces Drilldown source on
2026-11-08. Parsing a query does not imply that oteldb can execute it.

References:

- [Tempo grammar at 0b2b3c3](https://github.com/grafana/tempo/blob/0b2b3c361d38243367c368abe58642f88bdee7cf/pkg/traceql/expr.y).
- [Tempo hint definitions](https://github.com/grafana/tempo/blob/0b2b3c361d38243367c368abe58642f88bdee7cf/pkg/traceql/enum_hints.go).
- [Drilldown metrics query generator at 83d1146](https://github.com/grafana/traces-drilldown/blob/83d11467e87a49688034c522b413f48ec302b1ff/src/components/Explore/queries/generateMetricsQuery.ts).
- [Drilldown histogram query](https://github.com/grafana/traces-drilldown/blob/83d11467e87a49688034c522b413f48ec302b1ff/src/components/Explore/queries/histogram.ts).
- [Drilldown comparison selection](https://github.com/grafana/traces-drilldown/blob/83d11467e87a49688034c522b413f48ec302b1ff/src/components/Explore/queries/comparisonQuery.ts).
- [Drilldown exception search](https://github.com/grafana/traces-drilldown/blob/83d11467e87a49688034c522b413f48ec302b1ff/src/components/Explore/queries/exceptions.ts).

## Language coverage

| Feature | Lexer/parser/AST | Execution |
| --- | --- | --- |
| Span filters, scoped attributes and intrinsics, structural spanset operators | Already present | Search engine exists; some intrinsics remain unsupported. |
| `rate()`, `count_over_time()` | Already present | Missing metrics evaluator. |
| `min_over_time`, `max_over_time`, `sum_over_time`, `avg_over_time` | Already present | Missing metrics evaluator. |
| `quantile_over_time`, including multiple quantiles | Already present | Missing metrics evaluator and quantile result construction. |
| `histogram_over_time` | Already present | Missing histogram aggregation and bucket output. |
| Metrics `by(...)`, `topk`, `bottomk`, numeric result filters | Already present | Missing grouping and second-stage execution. |
| `compare({selection}, topN, start, end)` | Already present, including nanosecond selection bounds | Missing baseline/selection aggregation and comparison labels. |
| Metrics query arithmetic and simple constant operands | Already present | Missing metrics evaluator. |
| Trailing `with(name=static, ...)` hints | Added in this change | Explicitly rejected until implemented. |
| Compound constant operands and thresholds, e.g. `({} \| rate()) > 1 / 2` | Missing | Needs Tempo-compatible integer/float/duration folding. |
| Unary negation of parenthesized constants or metrics operands | Missing in metrics arithmetic | Needs parser and AST lowering. |

The metrics functions Drilldown emits are already represented by the AST.
The immediate syntax blocker was hints: sampled rate/latency breakdowns and
histograms append `with(sample=true)`, while exception searches append
`with(most_recent=true)`. Parser regression tests expand Grafana variables
into concrete selectors; they do not execute the upstream TypeScript generator.

## Hint representation

`Parse` preserves its existing expression types for queries without hints.
For queries with hints it returns `*HintedExpr`, containing the original
expression and an ordered `[]Hint` of names and typed `Static` values.

Names are identifiers and values use the existing static-literal grammar.
Unknown names and duplicate names are preserved for the future evaluator.
Like Tempo, a later `with(...)` block replaces an earlier block. Hints apply
to the complete query; they cannot occur inside a metrics arithmetic operand.
They are not spanset or metrics pipeline stages.

Matcher extraction, necessary trace-duration predicates, and search attribute
projection traverse the wrapper. Hints do not become storage matchers.
The current search evaluator returns `UnsupportedError` for hinted queries,
mapped to HTTP 501. It does not silently ignore sampling or ordering options.

## Remaining work for Drilldown

Both `TempoAPI.QueryRange` and `TempoAPI.Query` currently validate the time range
and return `ht.ErrNotImplemented`. The OpenAPI specification already defines
the metrics routes, labels, samples, exemplars, and partial-result fields.
There is no metrics execution path in `traceqlengine.BuildExpr` yet.

Prioritize the remaining work in this order:

1. Add a bounded metrics evaluator for `rate` and `count_over_time`, including
   time buckets, span timestamp boundaries, grouping, and native-storage reads.
   Wire the range and instant endpoints and verify Grafana response compatibility.
2. Add latency aggregations: min/max/sum/average, multiple quantiles, and
   histograms. Match Tempo's duration units, bucket labels, and empty-bucket
   behavior with reference tests.
3. Add exemplars for navigation back to traces, and `compare` selection/baseline
   results for the investigation workflow. Implement top/bottom-k, result filters,
   and metrics arithmetic alongside the aggregations that use them.
4. Define supported hint behavior. `most_recent` requires ordering before
   limiting search results; sampling requires explicit accuracy and weighting
   semantics. Preserve unknown hints without copying Tempo's backend-specific
   tuning knobs into oteldb configuration.
5. Finish constant-expression compatibility and test a running Drilldown plugin,
   including tag autocomplete, streaming transport, and result frame conversion.

Search-query execution, sampling, and metrics aggregation are outside this
parser change. Issue [#806](https://github.com/oteldb/oteldb/issues/806) remains open.
