package storagebackend_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// genSpreadLogs makes n records spread evenly over span, rotating severities, so several output
// steps have data for a range aggregation.
func genSpreadLogs(n int, start time.Time, span time.Duration) plog.Logs {
	levels := []struct {
		num  plog.SeverityNumber
		text string
	}{
		{plog.SeverityNumberInfo, "INFO"},
		{plog.SeverityNumberWarn, "WARN"},
		{plog.SeverityNumberError, "ERROR"},
		{plog.SeverityNumberDebug, "DEBUG"},
	}
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	recs := rl.ScopeLogs().AppendEmpty().LogRecords()
	gap := span / time.Duration(n)
	for i := 0; i < n; i++ {
		lv := levels[i%len(levels)]
		r := recs.AppendEmpty()
		r.SetTimestamp(pcommon.Timestamp(start.Add(time.Duration(i) * gap).UnixNano()))
		r.SetSeverityNumber(lv.num)
		r.SetSeverityText(lv.text)
		r.Body().SetStr(fmt.Sprintf(`{"level":%q,"i":%d}`, lv.text, i))
	}
	return ld
}

// runMetric evaluates a metric query and normalizes the matrix to series-label -> ts -> value.
func runMetric(t *testing.T, ctx context.Context, backend *storagebackend.Backend, query string, bucketed bool, start, end time.Time, step time.Duration) map[string]map[float64]string {
	t.Helper()
	var opts logqlengine.Options
	if bucketed {
		opts.Optimizers = []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}}
	}
	engine, err := logqlengine.NewEngine(backend.Logs(), opts)
	require.NoError(t, err)
	q, err := engine.NewQuery(ctx, query)
	require.NoError(t, err)
	data, err := q.Eval(ctx, logqlengine.EvalParams{
		Start: start, End: end, Step: step,
		Direction: logqlengine.DirectionForward, Limit: -1,
	})
	require.NoError(t, err)
	require.Equal(t, lokiapi.MatrixResultQueryResponseData, data.Type, query)

	out := map[string]map[float64]string{}
	for _, s := range data.MatrixResult.Result {
		key := fmt.Sprint(s.Metric.Or(lokiapi.LabelSet{}))
		m := map[float64]string{}
		for _, p := range s.Values {
			m[p.T] = p.V
		}
		out[key] = m
	}
	return out
}

// TestBucketedSamplingMatchesGeneric asserts the offloaded bucketed sampling path produces exactly
// the same matrix as the generic streaming RangeAggregation for the offloadable query shapes.
func TestBucketedSamplingMatchesGeneric(t *testing.T) {
	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)

	start := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	const span = 4 * time.Minute
	require.NoError(t, backend.ConsumeLogs(ctx, genSpreadLogs(4000, start, span)))

	end := start.Add(span)
	step := 30 * time.Second
	for _, query := range []string{
		`sum by (level) (count_over_time({service_name="api"}[1m]))`,
		`sum by (level) (rate({service_name="api"}[1m]))`,
		`sum(count_over_time({service_name="api"}[1m]))`,
		`sum(rate({service_name="api"}[2m]))`,
		`sum by (level) (bytes_over_time({service_name="api"}[1m]))`,
		`sum by (detected_level) (count_over_time({service_name="api"}[90s]))`,
	} {
		t.Run(query, func(t *testing.T) {
			generic := runMetric(t, ctx, backend, query, false, start, end, step)
			bucketed := runMetric(t, ctx, backend, query, true, start, end, step)
			require.Equal(t, generic, bucketed)
			require.NotEmpty(t, bucketed)
		})
	}
}
