package lokie2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// BenchmarkLogQLStreamSelector measures a resource-label stream selector over a high-cardinality
// dataset (many services). The embedded querier offloads the matcher to the postings index, so only
// the selected streams' records are fetched and materialized instead of every service's.
//
// The cases vary how much the selector prunes. Equality and a regexp selecting one service leave a
// single stream; an alternation leaves two; a match-all regexp leaves every stream, so it measures
// the pushdown when there is nothing to prune. MatchAllUnpushable (`!=` on a label no stream has)
// matches the empty string, so it cannot be pushed and is resolved after the fetch.
func BenchmarkLogQLStreamSelector(b *testing.B) {
	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)

	const (
		services   = 20
		perService = 300
	)
	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	for s := range services {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%02d", s))
		sl := rl.ScopeLogs().AppendEmpty()
		for r := range perService {
			rec := sl.LogRecords().AppendEmpty()
			rec.SetTimestamp(pcommon.Timestamp(now.Add(time.Duration(r) * time.Millisecond).UnixNano()))
			rec.Body().SetStr(fmt.Sprintf("GET /%d 200 svc-%02d", r, s))
		}
		require.NoError(b, backend.ConsumeLogs(ctx, ld))
	}

	engine, err := logqlengine.NewEngine(backend.Logs(), logqlengine.Options{
		ParseOptions: logql.ParseOptions{AllowDots: true},
	})
	require.NoError(b, err)

	params := logqlengine.EvalParams{
		Start:     now.Add(-time.Hour),
		End:       now.Add(time.Hour),
		Direction: logqlengine.DirectionForward,
		Limit:     10000,
	}
	for _, tt := range []struct {
		name, query string
	}{
		{"Equality", `{service_name="svc-07"}`},
		{"Regexp", `{service_name=~"svc-07"}`},
		{"RegexpAlternation", `{service_name=~"svc-0(7|8)"}`},
		{"RegexpMatchAll", `{service_name=~"svc-.+"}`},
		{"MatchAllUnpushable", `{service_name=~"svc-.+", missing!="x"}`},
	} {
		query, err := engine.NewQuery(ctx, tt.query)
		require.NoError(b, err)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := query.Eval(ctx, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
