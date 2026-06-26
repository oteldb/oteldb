package lokie2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// TestStorageBackend exercises the embedded github.com/oteldb/storage logs backend end-to-end:
// generated OTLP log batches are ingested through the storage ingestion sink (ConsumeLogs), then
// queried back over the full Loki HTTP API through the LogQL engine and the storage fetch seam.
//
// Like prome2e/TestStorageBackend it needs no ClickHouse or Docker — the storage engine runs fully
// in memory — so it runs as a normal unit test rather than behind E2E.
func TestStorageBackend(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider := integration.TraceProvider(t)

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	backend := storagebackend.New(store)

	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	set, err := generateLogs(now, 1)
	require.NoError(t, err)
	require.NotEmpty(t, set.Batches)
	for i, b := range set.Batches {
		if err := backend.ConsumeLogs(ctx, b); err != nil {
			t.Fatalf("Send batch %d: %+v", i, err)
		}
	}

	lq := backend.Logs()
	c := setupDB(t, provider, lq, lq)

	start := lokiapi.NewOptLokiTime(asLokiTime(set.Start))
	end := lokiapi.NewOptLokiTime(asLokiTime(set.End))

	// Stream labels come from resource and scope attributes of the ingested logs.
	t.Run("Labels", func(t *testing.T) {
		r, err := c.Labels(ctx, lokiapi.LabelsParams{Start: start, End: end})
		require.NoError(t, err)
		require.Contains(t, r.Data, "service_name")
		require.Contains(t, r.Data, "service_namespace")
	})

	t.Run("LabelValues", func(t *testing.T) {
		r, err := c.LabelValues(ctx, lokiapi.LabelValuesParams{
			Name:  "service_name",
			Start: start,
			End:   end,
		})
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"testService", "fooService"}, r.Data)
	})

	// A stream selector on a resource label returns the matching entries, all carrying that label.
	t.Run("StreamSelector", func(t *testing.T) {
		resp, err := c.QueryRange(ctx, lokiapi.QueryRangeParams{
			Query: `{service_name="testService"}`,
			Start: start,
			End:   end,
			Limit: lokiapi.NewOptInt(1000),
		})
		require.NoError(t, err)

		streams, ok := resp.Data.GetStreamsResult()
		require.True(t, ok)
		require.NotEmpty(t, streams.Result)

		var entries int
		for _, stream := range streams.Result {
			require.Equal(t, "testService", stream.Stream.Value["service_name"])
			entries += len(stream.Values)
		}
		require.NotZero(t, entries)
	})

	// A line filter is applied by the LogQL engine on top of the raw storage stream.
	t.Run("LineFilter", func(t *testing.T) {
		resp, err := c.QueryRange(ctx, lokiapi.QueryRangeParams{
			Query: `{service_name="fooService"} |= "POST"`,
			Start: start,
			End:   end,
			Limit: lokiapi.NewOptInt(1000),
		})
		require.NoError(t, err)

		streams, ok := resp.Data.GetStreamsResult()
		require.True(t, ok)
		require.NotEmpty(t, streams.Result)

		for _, stream := range streams.Result {
			for _, entry := range stream.Values {
				require.Contains(t, entry.V, "POST")
			}
		}
	})

	// A selector that matches no stream returns an empty result.
	t.Run("NoMatch", func(t *testing.T) {
		resp, err := c.QueryRange(ctx, lokiapi.QueryRangeParams{
			Query: `{service_name="clearly-not-exist"}`,
			Start: start,
			End:   end,
			Limit: lokiapi.NewOptInt(1000),
		})
		require.NoError(t, err)

		if streams, ok := resp.Data.GetStreamsResult(); ok {
			var entries int
			for _, stream := range streams.Result {
				entries += len(stream.Values)
			}
			require.Zero(t, entries)
		}
	})
}
