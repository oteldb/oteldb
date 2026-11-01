package prome2e_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
	prwpb "github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/promrw"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

type sparsePagedBackend struct {
	*backendfile.File

	mu        sync.Mutex
	readSizes []int
}

func (b *sparsePagedBackend) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	data, err := b.File.ReadAt(ctx, key, off, n)
	if err == nil && strings.HasSuffix(key, "/sidx") {
		b.mu.Lock()
		b.readSizes = append(b.readSizes, len(data))
		b.mu.Unlock()
	}
	return data, err
}

func (b *sparsePagedBackend) resetReads() {
	b.mu.Lock()
	clear(b.readSizes)
	b.readSizes = b.readSizes[:0]
	b.mu.Unlock()
}

func (b *sparsePagedBackend) sizes() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.readSizes)
}

func TestNativeSparsePagedQuery(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const base = int64(1700000000)
	dir := t.TempDir()
	files, err := backendfile.New(filepath.Join(dir, "parts"))
	require.NoError(t, err)
	be := &sparsePagedBackend{File: files}
	open := func() *storage.Storage {
		store, err := storage.Open(ctx, storage.Options{
			ReadCacheBytes: 0, DecodeCacheBytes: 1 << 20, DecodeMemoryBytes: 8 << 20,
		}, storage.WithBackend(be), storage.WithWALDir(filepath.Join(dir, "wal")), storage.WithFlushInterval(-1))
		require.NoError(t, err)
		return store
	}
	store := open()
	t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(ctx))) })
	handler := promrw.NewHandler(storagebackend.New(store), promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: 100 * 365 * 24 * time.Hour},
	})
	raw, err := sparsePagedWriteRequest(base).Marshal()
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(snappy.Encode(nil, raw)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.NoError(t, store.Admin().Flush(ctx, "default", signal.Metric))
	stats := store.Inspect()
	require.Len(t, stats.Tenants, 1)
	require.Len(t, stats.Tenants[0].Signals, 1)
	require.EqualValues(t, 4096, stats.Tenants[0].Signals[0].Series)
	require.Equal(t, 1, stats.Tenants[0].Signals[0].Parts)

	check := func(t *testing.T) {
		t.Helper()
		_, client := setupDB(t, integration.TraceProvider(t), storagebackend.New(store))
		const (
			system = `sum by(instance)(irate(sparse_paged_cpu_seconds_total{instance="host-0",job="node_exporter",mode="system"}[1m]))`
			all    = `sum by(instance)(irate(sparse_paged_cpu_seconds_total{instance="host-0",job="node_exporter"}[1m]))`
		)
		for _, tt := range []struct {
			name  string
			query string
			want  float64
			fine  bool
		}{
			{"ratio", system + " / on(instance) group_left " + all + " * 100", 200.0 / 3, true},
			{"numerator", system, 4, true},
			{"denominator", all, 6, false},
		} {
			t.Run(tt.name, func(t *testing.T) {
				be.resetReads()
				result, err := client.GetQueryRange(ctx, promapi.GetQueryRangeParams{
					Query: tt.query,
					Start: promapi.PrometheusTimestamp(strconv.FormatInt(base+37, 10)),
					End:   promapi.PrometheusTimestamp(strconv.FormatInt(base+67, 10)),
					Step:  promapi.NewOptString("15s"),
				})
				require.NoError(t, err)
				require.Equal(t, promapi.MatrixData, result.Data.Type)
				require.Len(t, result.Data.Matrix.Result, 1)
				points := result.Data.Matrix.Result[0].Values
				require.Len(t, points, 3)
				for _, point := range points {
					require.InDelta(t, tt.want, point.V, 1e-10)
				}
				sizes := be.sizes()
				fine := slices.Contains(sizes, 257*20) || slices.Contains(sizes, 256*20)
				coarse := slices.Contains(sizes, 1025*20) || slices.Contains(sizes, 1024*20)
				require.Equal(t, tt.fine, fine, "fine index page reads: %v", sizes)
				require.True(t, coarse, "default index page reads: %v", sizes)
			})
		}
	}
	t.Run("flushed", check)
	require.NoError(t, store.Close(ctx))
	store = open()
	t.Run("reopened", check)
}

func sparsePagedWriteRequest(base int64) *prwpb.WriteRequest {
	request := &prwpb.WriteRequest{}
	for host := range 1024 {
		for cpu := range 2 {
			for _, mode := range []string{"idle", "system"} {
				series := prwpb.TimeSeries{Labels: []prwpb.Label{
					{Name: "__name__", Value: "sparse_paged_cpu_seconds_total"},
					{Name: "cpu", Value: strconv.Itoa(cpu)},
					{Name: "instance", Value: "host-" + strconv.Itoa(host)},
					{Name: "job", Value: "node_exporter"},
					{Name: "mode", Value: mode},
				}}
				slope := 1.0
				if mode == "system" {
					slope = 2
				}
				for sample := range 5 {
					elapsed := int64(sample*15 + 7)
					series.Samples = append(series.Samples, prwpb.Sample{
						Timestamp: (base + elapsed) * 1000, Value: slope * float64(elapsed),
					})
				}
				request.Timeseries = append(request.Timeseries, series)
			}
		}
	}
	return request
}
