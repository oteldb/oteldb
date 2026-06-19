package lokie2e_test

import (
	"strings"
	"testing"
	"time"

	"github.com/go-faster/sdk/zctx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/integration/lokie2e"
	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// TestBucketedSampleSubMillisecondBoundary guards against a regression where
// chsql_stepfanout.go compared row timestamps at millisecond precision: rows
// placed at exact-second tick boundaries plus a sub-millisecond jitter (the
// shape produced by internal/lokicompliance/generator.go's GenerateLogs, used
// by the ch-logql-compliance suite) would round onto the wrong side of a
// window edge and get silently dropped or double-counted. Ticks every 1s,
// 5 interleaved lines per tick (round-robin across 3 methods), windows (1s)
// far narrower than the step (10s), mirrors that generator's shape.
func TestBucketedSampleSubMillisecondBoundary(t *testing.T) {
	integration.Skip(t)
	var (
		ctx         = t.Context()
		provider    = integration.TraceProvider(t)
		tablePrefix = strings.ReplaceAll(uuid.NewString(), "-", "")
	)
	_, c, tables := integration.SetupCH(t, integration.SetupCHOptions{
		Name:           "lokie2e-bucketed-sparse-repro",
		TablePrefix:    tablePrefix,
		TracerProvider: provider,
	})

	inserter, err := chstorage.NewInserter(c, chstorage.InserterOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	set := lokie2e.NewBatchSet()
	start := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	const (
		ticks = 30
		lines = 5
	)
	methods := []string{"GET", "POST", "PUT"}

	tick := start
	mi := 0
	for range ticks {
		rt := tick
		for range lines {
			rt = rt.Add(100 * time.Microsecond)
			method := methods[mi%len(methods)]
			mi++

			err := httpLog{
				ServiceName:      "test",
				ServiceNamespace: "test",
				Severity:         1,
				Time:             rt,
				Method:           method,
				Status:           200,
				Bytes:            10,
				Protocol:         "HTTP/1.1",
				IP:               "127.0.0.1",
			}.Append(set)
			require.NoError(t, err)
		}
		tick = tick.Add(time.Second)
	}
	consumer, err := logstorage.NewConsumer(inserter, logstorage.ConsumerOptions{})
	require.NoError(t, err)
	for _, b := range set.Batches {
		require.NoError(t, consumer.ConsumeLogs(ctx, b))
	}
	ctx = zctx.Base(ctx, integration.Logger(t))

	querier, err := chstorage.NewQuerier(c, chstorage.QuerierOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	offloaded := setupDB(t, provider, querier, querier)
	notOffloaded := setupDBWithOptimizers(t, provider, querier, querier, logqlengine.DefaultOptimizers())

	const step = 10 * time.Second
	queryStart := start
	queryEnd := start.Add(time.Duration(ticks) * time.Second)

	params := lokiapi.QueryRangeParams{
		Query: `sum by (http_method) (count_over_time({http_method=~".+"} [1s]))`,
		Start: lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(queryStart))),
		End:   lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(queryEnd))),
		Step:  lokiapi.NewOptPrometheusDuration(lokiapi.PrometheusDuration(step.String())),
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

	offloadedSeries := normalizeMatrix(t, matrixA)
	notOffloadedSeries := normalizeMatrix(t, matrixB)
	t.Logf("offloaded: %+v", offloadedSeries)
	t.Logf("notOffloaded: %+v", notOffloadedSeries)
	require.Equal(t, notOffloadedSeries, offloadedSeries)
}
