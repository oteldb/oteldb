package prome2e_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	storagepromql "github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/queryapi"
	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

type binopFetchSource struct {
	storagebackend.Source
	mu      sync.Mutex
	fetched map[string]int
}

func (s *binopFetchSource) Fetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return binopFetcher{source: s, Fetcher: s.Source.Fetcher(tenants...)}
}

type binopFetcher struct {
	fetch.Fetcher
	source *binopFetchSource
}

func (f binopFetcher) Fetch(ctx context.Context, req fetch.Request) (fetch.Iterator, error) {
	it, err := f.Fetcher.Fetch(ctx, req)
	if err != nil {
		return nil, err
	}
	return &binopFetchIterator{Iterator: it, source: f.source}, nil
}

type binopFetchIterator struct {
	fetch.Iterator
	source *binopFetchSource
}

func (it *binopFetchIterator) Next(ctx context.Context) (*fetch.Batch, error) {
	b, err := it.Iterator.Next(ctx)
	if err == nil {
		it.source.mu.Lock()
		it.source.fetched[storagepromql.PromLabels(b.Series).Get("__name__")]++
		it.source.mu.Unlock()
	}
	return b, err
}

func binopFileFixture(tb testing.TB, state string, nodes int) (*storage.Storage, time.Time) {
	tb.Helper()
	dir := tb.TempDir()
	files, err := backendfile.New(filepath.Join(dir, "parts"))
	require.NoError(tb, err)
	store, err := storage.Open(tb.Context(), storage.Options{}, storage.WithBackend(files),
		storage.WithWALDir(filepath.Join(dir, "wal")), storage.WithFlushInterval(-1))
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, store.Close(context.WithoutCancel(tb.Context()))) })
	base := time.Unix(2_000_000, 0).UTC()
	metrics := pmetric.NewMetrics()
	ms := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	for _, name := range []string{"join_cpu", "join_meta", "join_meta_alt"} {
		metric := ms.AppendEmpty()
		metric.SetName(name)
		gauge := metric.SetEmptyGauge()
		count := nodes
		if name == "join_cpu" {
			count = 3
		}
		for i := range count {
			instance := fmt.Sprintf("node%d", i)
			if name == "join_cpu" {
				instance = "node0"
				if i == 2 {
					instance = "unmatched"
				}
			}
			for step := range 3 {
				point := gauge.DataPoints().AppendEmpty()
				point.Attributes().PutStr("instance", instance)
				point.Attributes().PutStr("job", "nodes")
				if name == "join_cpu" {
					point.Attributes().PutStr("cpu", fmt.Sprint(i))
				} else {
					point.Attributes().PutStr("node", instance)
				}
				point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(step) * 15 * time.Second)))
				point.SetDoubleValue(float64(i + 2))
			}
		}
	}
	require.NoError(tb, storagebackend.New(store).ConsumeMetrics(tb.Context(), metrics))
	if state == "parts" {
		require.NoError(tb, store.Admin().Flush(tb.Context(), "", signal.Metric))
		require.NoError(tb, store.Close(tb.Context()))
		store, err = storage.Open(tb.Context(), storage.Options{}, storage.WithBackend(files),
			storage.WithWALDir(filepath.Join(dir, "wal")), storage.WithFlushInterval(-1))
		require.NoError(tb, err)
	}
	return store, base
}

func TestScarecrowBinopNarrowingNativeFileHTTP(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"head", "parts"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			store, base := binopFileFixture(t, state, 16)
			for _, tt := range []struct {
				query             string
				cpu, meta, series int
				factor            float64
			}{
				{`join_cpu * on(instance) group_left(node) join_meta`, 2, 1, 2, 2},
				{`join_meta * on(instance) group_right(node) join_cpu`, 2, 1, 2, 2},
				{`join_cpu < bool on(instance) group_left(node) join_meta`, 2, 1, 2, 0},
				{`join_cpu * on(instance,job) group_left(node) join_meta`, 3, 16, 2, 2},
				{`join_cpu and on(instance) join_meta`, 3, 16, 2, 1},
			} {
				t.Run(tt.query, func(t *testing.T) {
					source := &binopFetchSource{Source: store, fetched: make(map[string]int)}
					handler, err := queryapi.NewPrometheus(queryapi.PrometheusOptions{Querier: storagebackend.NewQuery(source)})
					require.NoError(t, err)
					server := httptest.NewServer(handler)
					t.Cleanup(server.Close)
					client, err := promapi.NewClient(server.URL, promapi.WithClient(server.Client()))
					require.NoError(t, err)
					response, err := client.GetQueryRange(t.Context(), promapi.GetQueryRangeParams{
						Query: tt.query, Start: getPromTS(pcommon.NewTimestampFromTime(base)),
						End: getPromTS(pcommon.NewTimestampFromTime(base.Add(30 * time.Second))), Step: promapi.NewOptString("15s"),
					})
					require.NoError(t, err)
					require.Equal(t, promapi.MatrixData, response.Data.Type)
					require.Len(t, response.Data.Matrix.Result, tt.series)
					for _, series := range response.Data.Matrix.Result {
						value, ok := map[string]float64{"0": 2, "1": 3}[series.Metric["cpu"]]
						require.True(t, ok)
						require.Len(t, series.Values, 3)
						for _, point := range series.Values {
							require.Equal(t, value*tt.factor, point.V)
						}
					}
					require.Equal(t, tt.cpu, source.fetched["join_cpu"])
					require.Equal(t, tt.meta, source.fetched["join_meta"])
				})
			}
		})
	}
}

func BenchmarkBinopNarrowingNativeFile(b *testing.B) {
	for _, state := range []string{"head", "parts"} {
		b.Run(state, func(b *testing.B) {
			store, base := binopFileFixture(b, state, 1_000)
			backend := storagebackend.New(store)
			engine := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(promstorage.Queryable) scarecrow.Scanner {
				return backend.ScarecrowScanner()
			}})
			for _, tt := range []struct{ name, query string }{
				{"sparse", `join_cpu * on(instance) group_left(node) join_meta`},
				{"dense", `join_meta * on(instance) join_meta`},
				{"dense_distinct", `join_meta * on(instance) join_meta_alt`},
				{"unchanged", `join_meta + join_meta`},
			} {
				b.Run(tt.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						q, err := engine.NewRangeQuery(b.Context(), nil, nil, tt.query, base, base.Add(30*time.Second), 15*time.Second)
						require.NoError(b, err)
						require.NoError(b, q.Exec(b.Context()).Err)
						q.Close()
					}
				})
			}
		})
	}
}
