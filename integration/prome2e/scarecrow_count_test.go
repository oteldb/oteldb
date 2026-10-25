package prome2e_test

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/promhandler"
	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestScarecrowStorageCountProjection(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
	backend := storagebackend.New(store)
	set := loadTestData(ctx, t, backend)
	engines := []promhandler.Engine{
		scarecrow.NewEngine(scarecrow.Opts{EnableAtModifier: true, MaxSamples: 1_000_000, Timeout: time.Minute, NewScanner: func(promstorage.Queryable) scarecrow.Scanner { return backend.ScarecrowScanner() }}),
		promql.NewEngine(promql.EngineOpts{EnableAtModifier: true, MaxSamples: 1_000_000, Timeout: time.Minute, LookbackDelta: 5 * time.Minute}),
	}
	clients := make([]*promapi.Client, len(engines))
	for i, engine := range engines {
		handler, err := promapi.NewServer(promhandler.NewPromAPI(engine, backend, backend, backend, promhandler.PromAPIOptions{}))
		require.NoError(t, err)
		server := httptest.NewServer(httpmiddleware.Wrap(handler, promhandler.PatchForm))
		t.Cleanup(server.Close)
		clients[i], err = promapi.NewClient(server.URL, promapi.WithClient(server.Client()))
		require.NoError(t, err)
	}
	const count = `count(count by(handler)(prometheus_http_requests_total))`
	for _, query := range []string{
		count,
		count + ` / scalar(` + count + `)`,
		fmt.Sprintf(`count by(handler)(prometheus_http_requests_total @ %d)`, set.End.AsTime().Unix()),
	} {
		t.Run(query, func(t *testing.T) {
			params := promapi.GetQueryRangeParams{Query: query, Start: getPromTS(pcommon.Timestamp(set.Start.AsTime().Add(-10 * time.Minute).UnixNano())), End: getPromTS(pcommon.Timestamp(set.End.AsTime().Add(10 * time.Minute).UnixNano())), Step: promapi.NewOptString("30s")}
			got, err := clients[0].GetQueryRange(ctx, params)
			require.NoError(t, err)
			want, err := clients[1].GetQueryRange(ctx, params)
			require.NoError(t, err)
			require.Equal(t, promapi.MatrixData, got.Data.Type)
			require.Equal(t, promapi.MatrixData, want.Data.Type)
			require.NotEmpty(t, want.Data.Matrix.Result)
			require.ElementsMatch(t, want.Data.Matrix.Result, got.Data.Matrix.Result)
		})
	}
}
