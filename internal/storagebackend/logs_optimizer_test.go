package storagebackend_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// TestLogQLOptimizerEquivalence verifies that pushing line filters into the storage fetch (via
// LogQLOptimizer) returns exactly the same results as evaluating the whole pipeline in the engine,
// across log and metric queries and offloadable/non-offloadable filter shapes.
func TestLogQLOptimizerEquivalence(t *testing.T) {
	b, ctx := newBackend(t)

	ts := time.Now().Truncate(time.Second)
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	sl := rl.ScopeLogs().AppendEmpty()
	bodies := []string{
		"GET /a 200", "POST /b 200", "GET /c 404", "HEAD /d 200",
		"GET /a 500", "DELETE /e 200", "xGETy embedded", "get lowercase",
	}
	for i, body := range bodies {
		rec := sl.LogRecords().AppendEmpty()
		rec.SetTimestamp(pcommon.Timestamp(ts.Add(time.Duration(i) * time.Millisecond).UnixNano()))
		rec.Body().SetStr(body)
	}
	require.NoError(t, b.ConsumeLogs(ctx, ld))

	start, end := ts.Add(-time.Hour), ts.Add(time.Hour)
	params := logqlengine.EvalParams{Start: start, End: end, Step: time.Minute, Limit: 1000}

	run := func(t *testing.T, optimize bool, query string) lokiapi.QueryResponseData {
		t.Helper()
		var opts logqlengine.Options
		opts.ParseOptions = logql.ParseOptions{AllowDots: true}
		if optimize {
			opts.Optimizers = []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}}
		}
		engine, err := logqlengine.NewEngine(b.Logs(), opts)
		require.NoError(t, err)
		q, err := engine.NewQuery(ctx, query)
		require.NoError(t, err)
		data, err := q.Eval(ctx, params)
		require.NoError(t, err)
		return data
	}

	for _, query := range []string{
		`{service_name="api"}`,
		`{service_name="api"} |= "GET"`,                       // offloaded: positive substring
		`{service_name="api"} |= "GET" |= "404"`,              // two offloaded filters (AND)
		`{service_name="api"} != "GET"`,                       // not offloaded (negated)
		`{service_name="api"} |~ "GET|POST"`,                  // not offloaded (regexp)
		`{service_name="api"} |= "GET" != "404"`,              // mixed
		`count_over_time({service_name="api"} |= "GET" [1h])`, // metric query with offloaded filter
	} {
		t.Run(query, func(t *testing.T) {
			require.Equal(t,
				normalize(t, run(t, false, query)),
				normalize(t, run(t, true, query)),
				"offloaded result must equal engine-only result",
			)
		})
	}
}

// normalize flattens a query response into a comparable, order-independent form.
func normalize(t *testing.T, data lokiapi.QueryResponseData) []string {
	t.Helper()
	var out []string
	for _, s := range data.StreamsResult.Result {
		for _, e := range s.Values {
			out = append(out, "line:"+e.V)
		}
	}
	for _, s := range data.MatrixResult.Result {
		ls := s.Metric.Value
		keys := make([]string, 0, len(ls))
		for k := range ls {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var lb strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&lb, "%s=%s,", k, ls[k])
		}
		for _, v := range s.Values {
			out = append(out, fmt.Sprintf("%s@%g=%s", lb.String(), v.T, v.V))
		}
	}
	sort.Strings(out)
	return out
}
