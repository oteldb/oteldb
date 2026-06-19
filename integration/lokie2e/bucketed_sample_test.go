package lokie2e_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/go-faster/sdk/zctx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
)

// TestBucketedSampleOffload verifies that pushing step-bucketing for LogQL
// range-aggregation offload (sum by(...) (count_over_time/rate/bytes_over_time/bytes_rate(...)))
// into ClickHouse SQL produces results equivalent to the non-offloaded,
// Go-side bucketing path (logqlmetric.RangeAggregation), including when
// step < range — which forces a single raw log line to contribute to
// multiple output steps.
//
// Values are compared with a small floating-point tolerance rather than
// exact equality: the non-offloaded path computes rate/bytes_rate per raw
// log line (each line is its own group, since RangeAggregationExpr has no
// grouping of its own) and only sums those per-line results in the outer
// `sum by(...)`, while the offloaded path sums raw samples in SQL first and
// divides once. Both are mathematically correct; summing many small
// divisions in a different order is expected to differ in the last few
// bits of a float64.
func TestBucketedSampleOffload(t *testing.T) {
	integration.Skip(t)
	var (
		ctx         = t.Context()
		provider    = integration.TraceProvider(t)
		tablePrefix = strings.ReplaceAll(uuid.NewString(), "-", "")
	)
	_, c, tables := integration.SetupCH(t, integration.SetupCHOptions{
		Name:           "lokie2e-bucketed-sample-offload",
		TablePrefix:    tablePrefix,
		TracerProvider: provider,
	})

	inserter, err := chstorage.NewInserter(c, chstorage.InserterOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)
	set := loadTestData(ctx, t, inserter)
	ctx = zctx.Base(ctx, integration.Logger(t))

	querier, err := chstorage.NewQuerier(c, chstorage.QuerierOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	// offloaded uses the ClickhouseOptimizer, exercising the new
	// BucketedSampleQuery SQL path.
	offloaded := setupDB(t, provider, querier, querier)
	// notOffloaded uses only the default optimizers (no ClickhouseOptimizer),
	// so range aggregation falls back to fetching raw samples and bucketing
	// them in Go (logqlmetric.RangeAggregation), the historical behavior.
	notOffloaded := setupDBWithOptimizers(t, provider, querier, querier, logqlengine.DefaultOptimizers())

	for _, tt := range []struct {
		name  string
		query string
	}{
		{"CountOverTime", `sum by (http_method) (count_over_time({http_method=~".+"} [30s]))`},
		{"Rate", `sum by (http_method) (rate({http_method=~".+"} [30s]))`},
		{"BytesOverTime", `sum by (http_method) (bytes_over_time({http_method=~".+"} [30s]))`},
		{"BytesRate", `sum by (http_method) (bytes_rate({http_method=~".+"} [30s]))`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := lokiapi.QueryRangeParams{
				Query: tt.query,
				Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
				End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
				// step < range: forces a raw log line to fall into multiple
				// output steps, exercising the fan-out (arrayJoin) tier.
				Step:  lokiapi.NewOptPrometheusDuration("10s"),
				Limit: lokiapi.NewOptInt(1000),
			}

			gotOffloaded, err := offloaded.QueryRange(ctx, params)
			require.NoError(t, err)
			gotNotOffloaded, err := notOffloaded.QueryRange(ctx, params)
			require.NoError(t, err)

			matrixA, ok := gotOffloaded.Data.GetMatrixResult()
			require.True(t, ok)
			matrixB, ok := gotNotOffloaded.Data.GetMatrixResult()
			require.True(t, ok)

			require.NotEmpty(t, matrixA.Result)

			offloadedSeries := normalizeMatrix(t, matrixA)
			notOffloadedSeries := normalizeMatrix(t, matrixB)
			require.ElementsMatch(t, mapKeys(notOffloadedSeries), mapKeys(offloadedSeries))

			for method, want := range notOffloadedSeries {
				got := offloadedSeries[method]
				require.Lenf(t, got, len(want), "method %q", method)
				for i := range want {
					assert.InDeltaf(t, want[i].value, got[i].value, 1e-9,
						"method %q, point %d", method, i)
					assert.Equalf(t, want[i].t, got[i].t,
						"method %q, point %d", method, i)
				}
			}
		})
	}
}

type fpoint struct {
	t     float64
	value float64
}

func normalizeMatrix(t *testing.T, m lokiapi.MatrixResult) map[string][]fpoint {
	out := make(map[string][]fpoint, len(m.Result))
	for _, series := range m.Result {
		method := series.Metric.Value["http_method"]
		points := make([]fpoint, len(series.Values))
		for i, v := range series.Values {
			value, err := strconv.ParseFloat(v.V, 64)
			require.NoError(t, err)
			points[i] = fpoint{t: v.T, value: value}
		}
		out[method] = points
	}
	return out
}

func mapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
