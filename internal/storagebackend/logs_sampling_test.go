package storagebackend_test

import (
	"context"
	"fmt"
	"strconv"
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
func runMetric(t *testing.T, ctx context.Context, backend *storagebackend.Backend, query string, bucketed bool, start, end time.Time, step time.Duration) map[string]map[float64]float64 {
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

	out := map[string]map[float64]float64{}
	for _, s := range data.MatrixResult.Result {
		key := fmt.Sprint(s.Metric.Or(lokiapi.LabelSet{}))
		m := map[float64]float64{}
		for _, p := range s.Values {
			v, err := strconv.ParseFloat(p.V, 64)
			require.NoError(t, err)
			m[p.T] = v
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
	// The level label value is the severity number's name (e.g. "Error"), so selectors use that.
	cases := []struct {
		query    string
		wantData bool
	}{
		{`sum by (level) (count_over_time({service_name="api"}[1m]))`, true},
		{`sum by (level) (rate({service_name="api"}[1m]))`, true},
		{`sum(count_over_time({service_name="api"}[1m]))`, true},
		{`sum(rate({service_name="api"}[2m]))`, true},
		{`sum by (level) (bytes_over_time({service_name="api"}[1m]))`, true},
		{`sum by (detected_level) (count_over_time({service_name="api"}[90s]))`, true},
		// Selectors NOT fully pushed to the fetch (per-row label, regex, absent): the offload must
		// fall back to the filtering path, not skip the selector and over-count.
		{`sum by (level) (count_over_time({level="Error"}[1m]))`, true},
		{`sum(count_over_time({level=~"Error|Warn"}[1m]))`, true},
		{`sum by (level) (count_over_time({nonexistent="x"}[1m]))`, false},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			generic := runMetric(t, ctx, backend, tc.query, false, start, end, step)
			bucketed := runMetric(t, ctx, backend, tc.query, true, start, end, step)
			if tc.wantData {
				require.NotEmpty(t, bucketed)
			}
			require.Len(t, bucketed, len(generic))
			for key, gpoints := range generic {
				bpoints, ok := bucketed[key]
				require.Truef(t, ok, "series %q missing in bucketed", key)
				require.Len(t, bpoints, len(gpoints))
				for ts, gv := range gpoints {
					// Float aggregation is non-associative; the generic path even sums in
					// parallel order. Compare within tolerance, not bit-exact.
					require.InEpsilonf(t, gv, bpoints[ts], 1e-9, "series %q at %v", key, ts)
				}
			}
		})
	}
}
