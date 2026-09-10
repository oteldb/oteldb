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

// evenLevels rotates through every severity, which is the pessimistic corpus for a level query:
// the matched level is a sixth of the window and no part holds one level only.
var evenLevels = []int{0, 1, 2, 3, 4, 5}

// skewedLevels is the shape real telemetry has — mostly info, a warn in twelve, an error in fifty —
// which is where filtering a level before materialization is worth the most.
var skewedLevels = func() (out []int) {
	for range 45 {
		out = append(out, 2)
	}
	out = append(out, 3, 3, 3, 3, 4)

	return out
}()

// genBenchLogs builds n synthetic log records modeled on the benchmark's
// otelbench stream: one service, rotating severities, http.method/status_code
// attributes, and a JSON body carrying level/method/status — so line filters,
// json parsing and metric aggregation all exercise real data.
//
// mix is the level distribution, as indices into the severity table cycled over the records.
func genBenchLogs(n int, start time.Time, mix []int) plog.Logs {
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
	for i := range n {
		lv := levels[mix[i%len(mix)]]
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
	runLogsQueryBench(b, evenLevels, []benchQuery{
		{"select_service", `{service_name="api"}`, false},
		{"line_filter", `{service_name="api"} |= "GET"`, false},
		{"json_status", `{service_name="api"} | json | status>=400`, false},
		// A level selector is resolved case-insensitively and cannot be applied exactly by the fetch
		// (the level is the severity number when set, else the severity text — two columns), so every
		// surviving record is re-checked against it. The fetch still drops what the severity number
		// alone disproves.
		{"select_level", `{service_name="api", level="error"}`, false},
		{"select_level_regexp", `{service_name="api", level=~"e.+"}`, false},
		{"metric_count_by_level", `sum by (level) (count_over_time({service_name="api"}[1m]))`, true},
		{"metric_rate_by_level", `sum by (level) (rate({service_name="api"}[1m]))`, true},
	})
}

// BenchmarkLogsQuerySkewedLevels is [BenchmarkLogsQuery]'s level queries over a corpus skewed the
// way production logs are. The even corpus understates what filtering a level at the fetch is
// worth: there, five of six records are dropped, here forty-nine of fifty.
func BenchmarkLogsQuerySkewedLevels(b *testing.B) {
	runLogsQueryBench(b, skewedLevels, []benchQuery{
		{"select_level", `{service_name="api", level="error"}`, false},
		{"select_level_regexp", `{service_name="api", level=~"e.+"}`, false},
		{"metric_count_by_level", `sum by (level) (count_over_time({service_name="api"}[1m]))`, true},
	})
}

type benchQuery struct {
	name   string
	q      string
	metric bool
}

func runLogsQueryBench(b *testing.B, mix []int, queries []benchQuery) {
	const n = 50_000
	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)

	start := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	require.NoError(b, backend.ConsumeLogs(ctx, genBenchLogs(n, start, mix)))

	engine, err := logqlengine.NewEngine(backend.Logs(), logqlengine.Options{
		Optimizers: []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}},
	})
	require.NoError(b, err)

	end := start.Add(time.Duration(n) * time.Millisecond).Add(time.Minute)
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
			for b.Loop() {
				if _, err := q.Eval(ctx, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
