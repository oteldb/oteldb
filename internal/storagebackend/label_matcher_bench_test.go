package storagebackend_test

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func BenchmarkLabelMatcherQueries(b *testing.B) {
	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, store.Close(context.Background())) })
	backend := storagebackend.New(store)
	base := time.Unix(1_700_000_000, 0).UTC()
	ld := plog.NewLogs()
	for i := range 1000 {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", i))
		rl.Resource().Attributes().PutStr("job", "api")
		records := rl.ScopeLogs().AppendEmpty().LogRecords()
		for j := range 8 {
			r := records.AppendEmpty()
			r.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(j) * time.Millisecond).UnixNano()))
			r.Body().SetStr("request completed")
		}
	}
	require.NoError(b, backend.ConsumeLogs(b.Context(), ld))
	selector := logql.Selector{Matchers: []logql.LabelMatcher{
		{Label: "job", Op: logql.OpEq, Value: "api"},
		{Label: "service_name", Op: logql.OpRe, Value: "svc-(17|85)", Re: regexp.MustCompile(`^(?:svc-(17|85))$`)},
	}}
	start, end := base.Add(-time.Second), base.Add(time.Second)
	b.Run("series", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			got, err := backend.Logs().Series(b.Context(), logstorage.SeriesOptions{Start: start, End: end, Selectors: []logql.Selector{selector}})
			if err != nil || len(got) != 2 {
				b.Fatal("series", got, err)
			}
		}
	})
	b.Run("label_values", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			it, err := backend.Logs().LabelValues(b.Context(), "service_name", logstorage.LabelsOptions{Start: start, End: end, Query: selector})
			if err != nil {
				b.Fatal(err)
			}
			count := 0
			var label logstorage.Label
			for it.Next(&label) {
				count++
			}
			if err := it.Err(); err != nil || count != 2 {
				b.Fatal("labels", count, err)
			}
			if err := it.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("log_plan_eval", func(b *testing.B) {
		engine, err := logqlengine.NewEngine(backend.Logs(), logqlengine.Options{Optimizers: []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}}})
		require.NoError(b, err)
		params := logqlengine.EvalParams{Start: start, End: end, Limit: 100}
		b.ReportAllocs()
		for b.Loop() {
			q, err := engine.NewQuery(b.Context(), `{job="api", service_name=~"svc-(17|85)"}`)
			if err != nil {
				b.Fatal(err)
			}
			got, err := q.Eval(b.Context(), params)
			if err != nil || len(got.StreamsResult.Result) != 2 {
				b.Fatal("query", got, err)
			}
		}
	})
}
