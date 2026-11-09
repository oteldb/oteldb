# TraceQL count and rate queries

The native storage backend serves `count_over_time()` and `rate()` through
`GET /api/metrics/query_range` and `GET /api/metrics/query`. Grouping, scalar and
vector arithmetic, comparison filters, and top/bottom-k use scarecrow's shared
column operators. These queries do not generate or persist metric series.

```traceql
{status=error} | rate() by(resource.service.name)
{} | count_over_time() by(span.http.route) | topk(10)
({status=error} | rate() by(resource.service.name)) / ({} | rate())
```

## Time and result semantics

Range queries accept `start`, `end`, `since`, and `step`. With no bounds they use
the last hour; a missing step targets about 240 buckets. Explicit steps must be
positive whole milliseconds. A zero-width or reversed interval is rejected.

Matching Tempo, range bounds align to the step and include an additional initial
bucket. A bucket is right-closed `(left, right]`; the returned timestamp is its
right edge in milliseconds. Span selection uses nanosecond timestamps, so spans
on opposite sides of an edge do not collapse when the API timestamp is rounded.
Aligned buckets may include spans outside the original, unaligned request bounds.

Instant queries aggregate the inclusive `[start, end]` interval into one value per
series. `rate()` divides the count by the bucket width in seconds, or by the full
interval for instant queries. It is not a PromQL counter rate. Multiple spans
sharing a timestamp remain separate observations.

An ungrouped empty selection produces zeros. Grouped empty selections produce no
series. Existing groups have zero-valued empty buckets. Arithmetic/filter/ranking
gaps are omitted from the response; division by zero creates a gap. Typed grouping
identities distinguish strings, integers, numbers, and booleans. Missing group
attributes are omitted from response labels and remain distinct internally from
literal strings such as `"nil"`.

## Execution and bounds

`traceqlengine.Engine.EvalMetrics` parses the query, builds a bucket grid and
first-stage source, and runs `BuildMetrics`. The source groups observations once;
the shared runtime handles result operators. `storagebackend.TraceQuerier`
implements the optional `MetricsQuerier` capability.

Simple span filters stream projected batches, materializing only requested
columns. A bare count does not decode span attrs, events, links, names, or
durations. Resource and instrumentation values come from stream identity.
Filters are rechecked after index pushdown. Native batches are released after
their selected fields have been copied into the current iterator batch.

Structural pipelines and trace-scoped fields fetch complete candidate traces
and run the existing TraceQL processors. Only the selected spans' start times
decide their metrics bucket. Parents and root metadata may lie outside the
metrics interval; search's whole-trace containment rule does not apply here.
The structural fallback still materializes candidate traces under the native
query read budget; it is not the projected streaming fast path.

One native read/decode budget is shared across operands. Additional engine options
bound the evaluation:

| Option | Default | Scope |
| --- | ---: | --- |
| `MetricsMaxSteps` | 11,000 | Aligned output grid |
| `MetricsMaxSeries` | 10,000 | First-stage groups across operands |
| `MetricsMaxSamples` | 1,000,000 | Allocated bucket values across operands |
| `MetricsMaxSpans` | 1,000,000 | Inspected spans across operands |

There are at most five grouping attributes, and retained encoded grouping labels
are capped at 8 MiB per query. Invalid parameters return 400, resource refusals
return 422, and unsupported operations/backends return 501. Decoder errors and
execution errors use the same Tempo error schema.

## Remaining capabilities

Only the native backend currently implements `MetricsQuerier`. Duration
aggregations, histograms, quantiles, `compare()`, sampling hints, and event/link
grouping remain unsupported. Supported grouping values use the existing
TraceQL scalar attribute model. Exemplars are not generated; the requested
exemplar maximum may legitimately return none. Infinite numeric results are
rejected rather than emitting invalid JSON. Full Grafana Drilldown UI compatibility
is not claimed by the HTTP integration tests.

The integration suite ingests a small OTLP fixture and queries the actual HTTP
endpoints over head data, flushed parts, and reopened file storage:

```sh
E2E=1 go test -race ./integration/tempoe2e -run 'TestStorageBackendMetrics|TestMetricsHTTP'
```

Reference: [Tempo interval mapping and alignment](https://github.com/grafana/tempo/blob/0b2b3c361d38243367c368abe58642f88bdee7cf/pkg/traceql/engine_metrics.go).
