package prome2e_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend/faultbackend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
	prwpb "github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/promrw"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestNativeCompactionDuringFlush(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	probe := &nativeTenureProbe{SpanRecorder: tracetest.NewSpanRecorder(), entered: make(chan struct{}, 1)}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(probe))
	t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })
	dir := t.TempDir()
	files, err := backendfile.New(filepath.Join(dir, "parts"))
	require.NoError(t, err)
	be := faultbackend.Wrap(files)
	open := func() *storage.Storage {
		store, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(be),
			storage.WithWALDir(filepath.Join(dir, "wal")), storage.WithFlushInterval(-1),
			storage.WithTracerProvider(tp))
		require.NoError(t, err)
		return store
	}
	store := open()
	t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(ctx))) })
	handler := promrw.NewHandler(storagebackend.New(store), promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: 100 * 365 * 24 * time.Hour},
	})
	for generation := range 2 {
		for row := range 8 {
			writeMaintenancePoint(t, handler, 1700000000000+int64(generation*8000+row*1000))
		}
		require.NoError(t, store.Admin().Flush(ctx, "default", signal.Metric))
	}
	writeMaintenancePoint(t, handler, 1700000100000)
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") }))
	flushed := make(chan error, 1)
	flushFinished := make(chan struct{})
	go func() {
		defer close(flushFinished)
		flushed <- store.Admin().Flush(ctx, "default", signal.Metric)
	}()
	t.Cleanup(func() { release(); <-flushFinished })
	gate.Await(t)
	be.Reset()
	merged := make(chan error, 1)
	mergeFinished := make(chan struct{})
	go func() {
		defer close(mergeFinished)
		merged <- store.Admin().CompactNow(ctx, "default", signal.Metric)
	}()
	t.Cleanup(func() { release(); <-mergeFinished })
	select {
	case err := <-merged:
		require.NoError(t, err, "compaction must finish while the independent flush is paused")
	case <-probe.entered:
		t.Fatal("local compaction entered clustered tenure establishment")
	}
	release()
	require.NoError(t, <-flushed)
	require.NoError(t, store.Close(ctx))
	store = open()
	_, client := setupDB(t, tp, storagebackend.New(store))
	result, err := client.GetQuery(ctx, promapi.GetQueryParams{
		Query: "sum(count_over_time(maintenance_overlap[5m]))",
		Time:  promapi.NewOptPrometheusTimestamp("1700000110"),
	})
	require.NoError(t, err)
	require.Len(t, result.Data.Vector.Result, 1)
	value, ok := result.Data.Vector.Result[0].Value.HistogramOrValue.GetStringFloat64()
	require.True(t, ok)
	require.Equal(t, 17.0, value, "all remote-write samples survive overlapping maintenance and reopen")
}

func writeMaintenancePoint(t *testing.T, handler http.Handler, timestamp int64) {
	t.Helper()
	raw, err := (&prwpb.WriteRequest{Timeseries: []prwpb.TimeSeries{{
		Labels:  []prwpb.Label{{Name: "__name__", Value: "maintenance_overlap"}},
		Samples: []prwpb.Sample{{Timestamp: timestamp, Value: 1}},
	}}}).Marshal()
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(snappy.Encode(nil, raw)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
}

type nativeTenureProbe struct {
	*tracetest.SpanRecorder

	entered chan struct{}
}

func (p *nativeTenureProbe) OnStart(ctx context.Context, span sdktrace.ReadWriteSpan) {
	p.SpanRecorder.OnStart(ctx, span)
	if span.Name() == "engine.establishTenure" {
		select {
		case p.entered <- struct{}{}:
		default:
		}
	}
}
