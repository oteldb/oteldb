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
	records := []struct {
		body, level, method string
	}{
		{"GET /a 200", "info", "GET"},
		{"POST /b 200", "info", "POST"},
		{"GET /c 404", "error", "GET"},
		{"HEAD /d 200", "info", "HEAD"},
		{"GET /a 500", "error", "GET"},
		{"DELETE /e 200", "warn", "DELETE"},
		{"xGETy embedded", "info", "GET"},
		{"get lowercase", "info", "GET"},
		// A JSON body whose PARSED level ("debug") differs from the STORED level attr ("error"):
		// after `| json`, a `| level=...` filter must see "debug", so it must NOT be offloaded.
		{`{"level":"debug","method":"PUT"}`, "error", "PUT"},
	}
	for i, r := range records {
		rec := sl.LogRecords().AppendEmpty()
		rec.SetTimestamp(pcommon.Timestamp(ts.Add(time.Duration(i) * time.Millisecond).UnixNano()))
		rec.Body().SetStr(r.body)
		rec.Attributes().PutStr("level", r.level)        // clean record-attribute label
		rec.Attributes().PutStr("http.method", r.method) // dotted record-attribute label
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
		`{service_name="api"} |= "GET"`,                                   // offloaded: positive substring
		`{service_name="api"} |= "GET" |= "404"`,                          // two offloaded filters (AND)
		`{service_name="api"} != "GET"`,                                   // not offloaded (negated)
		`{service_name="api"} |~ "GET|POST"`,                              // not offloaded (regexp)
		`{service_name="api"} |= "GET" != "404"`,                          // mixed
		`count_over_time({service_name="api"} |= "GET" [1h])`,             // metric query with offloaded filter
		`{service_name="api"} | level = "error"`,                          // offloaded: clean record-attr label filter
		`{service_name="api"} |= "GET" | level = "info"`,                  // line filter + label filter, both offloaded
		`{service_name="api"} | level = "error" | level = "info"`,         // contradictory (empty), offloaded
		`{service_name="api"} | http_method = "GET"`,                      // dotted label filter, offloaded via LogKeys
		`{service_name="api"} | level != "info"`,                          // negated label filter, not offloaded
		`{service_name="api"} | label_format dummy="x" | level = "error"`, // label_format before: not offloaded
		`{service_name="api"} | logfmt | level = "error"`,                 // parser before: must NOT offload (parsed field)
		`{service_name="api"} | json | level = "debug"`,                   // parsed level=debug, must NOT offload stored level
		`{service_name="api"} | json | level = "error"`,                   // parsed level shadows stored; must NOT offload
		`count_over_time({service_name="api"} | level = "error" [1h])`,    // metric query with label filter
	} {
		t.Run(query, func(t *testing.T) {
			require.Equal(
				t,
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
