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
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// genBenchLogs builds n synthetic log records modelled on the benchmark's
// otelbench stream: one service, rotating severities, http.method/status_code
// attributes, and a JSON body carrying level/method/status — so line filters,
// json parsing and metric aggregation all exercise real data.
func genBenchLogs(n int, start time.Time) plog.Logs {
	levels := []struct {
		num  plog.SeverityNumber
		text string
	}{
		{plog.SeverityNumberTrace, "TRACE"},
		{plog.SeverityNumberDebug, "DEBUG"},
		{plog.SeverityNumberInfo, "INFO"},
		{plog.SeverityNumberWarn, "WARN"},
		{plog.SeverityNumberError, "ERROR"},
		{plog.SeverityNumberFatal, "FATAL"},
	}
	methods := []string{"GET", "POST", "PUT", "HEAD", "DELETE", "PATCH"}
	statuses := []int64{200, 201, 204, 400, 404, 500}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	sl := rl.ScopeLogs().AppendEmpty()
	recs := sl.LogRecords()
	recs.EnsureCapacity(n)
	for i := 0; i < n; i++ {
		lv := levels[i%len(levels)]
		method := methods[i%len(methods)]
		status := statuses[i%len(statuses)]
		ts := start.Add(time.Duration(i) * time.Millisecond)

		r := recs.AppendEmpty()
		r.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
		r.SetSeverityNumber(lv.num)
		r.SetSeverityText(lv.text)
		r.Body().SetStr(fmt.Sprintf(`{"level":%q,"method":%q,"status":%d,"client_ip":"10.0.0.%d"}`,
			lv.text, method, status, i%256))
		a := r.Attributes()
		a.PutStr("http.method", method)
		a.PutInt("http.status_code", status)
	}
	return ld
}

// BenchmarkLogsQuery exercises the embedded LogQL read path end-to-end (parse →
// plan → fetch → materialize → eval) over a fixed corpus, one sub-benchmark per
// representative suite query. Apple-to-apple with the docker benchmark's queries,
// but in-process so it profiles in seconds.
func BenchmarkLogsQuery(b *testing.B) {
	const n = 50_000
	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)

	start := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	require.NoError(b, backend.ConsumeLogs(ctx, genBenchLogs(n, start)))

	engine, err := logqlengine.NewEngine(backend.Logs(), logqlengine.Options{
		Optimizers: []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}},
	})
	require.NoError(b, err)

	end := start.Add(time.Duration(n) * time.Millisecond).Add(time.Minute)
	queries := []struct {
		name   string
		q      string
		metric bool
	}{
		{"select_service", `{service_name="api"}`, false},
		{"line_filter", `{service_name="api"} |= "GET"`, false},
		{"json_status", `{service_name="api"} | json | status>=400`, false},
		{"metric_count_by_level", `sum by (level) (count_over_time({service_name="api"}[1m]))`, true},
		{"metric_rate_by_level", `sum by (level) (rate({service_name="api"}[1m]))`, true},
	}

	for _, tc := range queries {
		b.Run(tc.name, func(b *testing.B) {
			params := logqlengine.EvalParams{
				Start:     start,
				End:       end,
				Direction: logqlengine.DirectionBackward,
				Limit:     1000,
			}
			if tc.metric {
				params.Step = 30 * time.Second
			}
			q, err := engine.NewQuery(ctx, tc.q)
			require.NoError(b, err)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := q.Eval(ctx, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
