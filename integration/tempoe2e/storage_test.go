package tempoe2e_test

import (
	"context"
	"io"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

// TestStorageBackend exercises the embedded github.com/oteldb/storage traces backend end-to-end:
// real OTLP trace batches are ingested through the storage ingestion sink (ConsumeTraces), then
// queried back over the full Tempo HTTP API through the TraceQL engine and the storage fetch seam.
//
// Like prome2e/TestStorageBackend it needs no ClickHouse or Docker — the storage engine runs fully
// in memory — so it runs as a normal unit test rather than behind E2E.
func TestStorageBackend(t *testing.T) {
	t.Parallel()

	var (
		ctx      = context.Background()
		provider = integration.TraceProvider(t)
	)

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	backend := storagebackend.New(store)

	set, err := readBatchSet()
	require.NoError(t, err)
	require.NotEmpty(t, set.Batches)
	require.NotEmpty(t, set.Traces)
	for i, b := range set.Batches {
		if err := backend.ConsumeTraces(ctx, b); err != nil {
			t.Fatalf("Send batch %d: %+v", i, err)
		}
	}

	tq := backend.Traces()
	c := setupDB(t, provider, tq, tq)

	// Every ingested trace is fetchable by id and returns all of its spans.
	t.Run("TraceByID", func(t *testing.T) {
		for traceID, trace := range set.Traces {
			r, err := c.TraceByID(ctx, tempoapi.TraceByIDParams{
				TraceID: otelstorage.TraceID(traceID).Hex(),
			})
			require.NoError(t, err)
			require.IsType(t, &tempoapi.TraceByID{}, r)

			data, err := io.ReadAll(r.(*tempoapi.TraceByID))
			require.NoError(t, err)

			var u ptrace.ProtoUnmarshaler
			resp, err := u.UnmarshalTraces(data)
			require.NoError(t, err)
			require.Equal(t, len(trace.Spanset), resp.SpanCount(), "trace %s span count", traceID)
		}
	})

	// SearchTags surfaces the resource and span attribute names that were ingested.
	t.Run("SearchTags", func(t *testing.T) {
		r, err := c.SearchTags(ctx, tempoapi.SearchTagsParams{})
		require.NoError(t, err)
		require.Contains(t, r.TagNames, "service.name")
		require.Contains(t, r.TagNames, "http.method")
	})

	// TraceQL search over storage must agree with the in-memory reference engine on the set of
	// matched traces, exercising the storage fetch + engine evaluation path.
	t.Run("SearchWithTraceQL", func(t *testing.T) {
		for _, query := range []string{
			`{ .http.method = "POST" && .http.status_code = 200 }`,
			`{ .http.method = "GET" && .http.status_code = 200 }`,
			`{ name = "list-articles" }`,
			`{ .http.method =~ "^POST$" && .http.status_code = 200 }`,
			`{ .service.name = "clearly-does-not-exist" }`,
		} {
			t.Run(query, func(t *testing.T) {
				r, err := c.Search(ctx, tempoapi.SearchParams{
					Q:     tempoapi.NewOptString(query),
					Limit: tempoapi.NewOptInt(1_000),
				})
				require.NoError(t, err)

				ref, err := set.Engine.Eval(ctx, query, traceqlengine.EvalParams{Limit: 1_000})
				require.NoError(t, err)

				require.ElementsMatch(t, apiTraceIDs(ref.Traces), apiTraceIDs(r.Traces),
					"storage and in-memory engine must match for %q", query)
			})
		}
	})
}

func apiTraceIDs(traces []tempoapi.TraceSearchMetadata) []string {
	ids := make([]string, 0, len(traces))
	for _, tr := range traces {
		ids = append(ids, tr.TraceID)
	}
	sort.Strings(ids)
	return ids
}
