package pyroe2e_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	querierv1 "github.com/grafana/pyroscope/api/gen/proto/go/querier/v1"
	"github.com/grafana/pyroscope/api/gen/proto/go/querier/v1/querierv1connect"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/profilehandler"
	"github.com/oteldb/oteldb/internal/profileql/profileqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestProfileTimelineNeedsNoSymbols(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	be := backend.Memory()
	store, err := storage.InMemory(storage.WithBackend(be), storage.WithProfileSymbolCache(-1))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(ctx))) })
	db := storagebackend.New(store)
	start := time.Date(2026, time.October, 24, 12, 0, 0, 0, time.UTC)
	data, end, total := generateProfiles(start, []string{"frontend", "backend"}, 120)
	require.NoError(t, db.ConsumeProfiles(ctx, data))
	require.NoError(t, store.Admin().Flush(ctx, "", signal.Profile))
	for _, part := range store.Parts("", signal.Profile) {
		require.NoError(t, be.Write(ctx, part.ID+"/sym-strings.bin", []byte("unreadable symbols")))
	}
	engine := profileqlengine.NewEngine(db.Profiles(), profileqlengine.Options{})
	svc := profilehandler.NewQuerierService(db.Profiles(), engine)
	path, handler := querierv1connect.NewQuerierServiceHandler(svc)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := querierv1connect.NewQuerierServiceClient(server.Client(), server.URL)
	for _, selector := range []string{"", `{service.name="frontend"}`} {
		req := connect.NewRequest(&querierv1.SelectSeriesRequest{ProfileTypeID: profileTypeID, LabelSelector: selector, Start: start.UnixMilli(), End: end.UnixMilli(), Step: 1})
		result, err := client.SelectSeries(ctx, req)
		require.NoError(t, err)
		require.Len(t, result.Msg.Series, 1)
		require.Len(t, result.Msg.Series[0].Points, 120)
		var sum int64
		for i, p := range result.Msg.Series[0].Points {
			require.Equal(t, start.Add(time.Duration(i)*time.Second).UnixMilli(), p.Timestamp)
			sum += int64(p.Value)
		}
		expected := total
		if selector != "" {
			expected /= 2
		}
		require.Equal(t, expected, sum)
	}
	_, err = engine.Select(ctx, profileTypeID, profileqlengine.EvalParams{Start: start, End: end})
	require.Error(t, err, "the flamegraph path still needs symbols")
}
