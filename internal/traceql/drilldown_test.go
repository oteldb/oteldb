package traceql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// These queries follow traces-drilldown's generateMetricsQuery and buildHistogramQuery,
// with Grafana variables expanded. See docs/traceql.md for the upstream revision.
var drilldownMetricsTests = []struct {
	name       string
	query      string
	op         MetricsOp
	field      *Attribute
	by         []Attribute
	parameters []float64
	sample     bool
}{
	{
		name:  "rate",
		query: `{kind=server && resource.service.name="checkout"} | rate()`,
		op:    MetricsOpRate,
	},
	{
		name:  "error rate",
		query: `{kind=server && resource.service.name="checkout" && status=error} | rate()`,
		op:    MetricsOpRate,
	},
	{
		name:       "duration percentiles",
		query:      `{kind=server && resource.service.name="checkout"} | quantile_over_time(duration, .5, .9, .99)`,
		op:         MetricsOpQuantileOverTime,
		field:      &Attribute{Prop: SpanDuration},
		parameters: []float64{.5, .9, .99},
	},
	{
		name:   "sampled service breakdown",
		query:  `{kind=server && resource.service.name!=nil} | rate() by(resource.service.name) with(sample=true)`,
		op:     MetricsOpRate,
		by:     []Attribute{{Scope: ScopeResource, Name: "service.name"}},
		sample: true,
	},
	{
		name:  "error route breakdown",
		query: `{kind=server && status=error && span.http.route!=nil} | rate() by(span.http.route)`,
		op:    MetricsOpRate,
		by:    []Attribute{{Scope: ScopeSpan, Name: "http.route"}},
	},
	{
		name:       "sampled latency breakdown",
		query:      `{kind=server && span.http.route!=nil} | quantile_over_time(duration, .99) by(span.http.route) with(sample=true)`,
		op:         MetricsOpQuantileOverTime,
		field:      &Attribute{Prop: SpanDuration},
		parameters: []float64{.99},
		by:         []Attribute{{Scope: ScopeSpan, Name: "http.route"}},
		sample:     true,
	},
	{
		name:   "latency histogram",
		query:  `{kind=server && resource.service.name="checkout"} | histogram_over_time(duration) with(sample=true)`,
		op:     MetricsOpHistogramOverTime,
		field:  &Attribute{Prop: SpanDuration},
		sample: true,
	},
	{
		name:  "pinned breakdown tile",
		query: `{kind=server && resource.service.name="checkout"} | rate() by(resource.service.name)`,
		op:    MetricsOpRate,
		by:    []Attribute{{Scope: ScopeResource, Name: "service.name"}},
	},
}

func TestParseDrilldownMetrics(t *testing.T) {
	t.Parallel()
	for _, tt := range drilldownMetricsTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			expr, err := Parse(tt.query)
			require.NoError(t, err)
			if tt.sample {
				hinted, ok := expr.(*HintedExpr)
				require.True(t, ok, "got %T", expr)
				require.Equal(t, []Hint{{Name: "sample", Value: Static{Type: TypeBool, Data: 1}}}, hinted.Hints)
				expr = hinted.Expr
			}
			agg, ok := expr.(*MetricsAggregation)
			require.True(t, ok, "got %T", expr)
			require.Equal(t, tt.op, agg.Op)
			require.Equal(t, tt.field, agg.Field)
			require.Equal(t, tt.by, agg.By)
			require.Equal(t, tt.parameters, agg.Parameters)
			_, matchers := ExtractMatchers(expr)
			require.NotEmpty(t, matchers)
		})
	}
}
