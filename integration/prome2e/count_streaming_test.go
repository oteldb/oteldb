package prome2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/oteldb/storage"
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

type countStreamingInput struct {
	host    string
	cpu     string
	samples []prwpb.Sample
}

func TestNativeStreamingRangeCounts(t *testing.T) {
	t.Parallel()
	const base = int64(1700000000)
	ctx := t.Context()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })
	dir := t.TempDir()
	files, err := backendfile.New(filepath.Join(dir, "parts"))
	require.NoError(t, err)
	open := func() *storage.Storage {
		store, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(files),
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
	inputs := countStreamingInputs(base)
	for part := range 3 {
		request := prwpb.WriteRequest{}
		for i, input := range inputs {
			series := prwpb.TimeSeries{Labels: []prwpb.Label{
				{Name: "__name__", Value: "count_streaming_cpu_seconds_total"},
				{Name: "cpu", Value: input.cpu},
				{Name: "instance", Value: input.host},
				{Name: "job", Value: "node_exporter"},
				{Name: "mode", Value: []string{"idle", "user"}[i%2]},
			}}
			for _, sample := range input.samples {
				if elapsed := sample.Timestamp/1000 - base; elapsed >= int64(part*400) && elapsed < int64((part+1)*400) {
					series.Samples = append(series.Samples, sample)
				}
			}
			if len(series.Samples) > 0 {
				request.Timeseries = append(request.Timeseries, series)
			}
		}
		raw, err := request.Marshal()
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(snappy.Encode(nil, raw)))
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "snappy")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
		require.NoError(t, store.Admin().Flush(ctx, "default", signal.Metric))
	}
	require.NoError(t, store.Admin().CompactNow(ctx, "default", signal.Metric))
	stats := store.Inspect()
	require.Len(t, stats.Tenants, 1)
	require.Len(t, stats.Tenants[0].Signals, 1)
	require.Equal(t, 1, stats.Tenants[0].Signals[0].Parts, "compaction makes a part spanning the evaluation windows")
	check := func(t *testing.T) {
		t.Helper()
		_, client := setupDB(t, tp, storagebackend.New(store))
		for _, host := range []string{"host-0", "host-7"} {
			t.Run(host, func(t *testing.T) {
				before := len(recorder.Ended())
				result, err := client.GetQueryRange(ctx, promapi.GetQueryRangeParams{
					Query: fmt.Sprintf(`count(count(count_streaming_cpu_seconds_total{instance=%q,job="node_exporter"}) by(cpu))`, host),
					Start: promapi.PrometheusTimestamp(strconv.FormatInt(base+300, 10)),
					End:   promapi.PrometheusTimestamp(strconv.FormatInt(base+1200, 10)),
					Step:  promapi.NewOptString("60s"),
				})
				require.NoError(t, err)
				require.Equal(t, promapi.MatrixData, result.Data.Type)
				require.Len(t, result.Data.Matrix.Result, 1)
				points := result.Data.Matrix.Result[0].Values
				require.Len(t, points, 16)
				for i, point := range points {
					at := base + 300 + int64(i)*60
					require.Equal(t, countStreamingExpected(inputs, host, at), point.V, "host=%s evaluation=%d", host, at-base)
				}
				grid := false
				for _, span := range recorder.Ended()[before:] {
					if span.Name() == "engine.countGrid" {
						grid = true
					}
				}
				require.True(t, grid, "the native timestamp-only count-grid route must execute")
			})
		}
	}
	t.Run("compacted", check)
	require.NoError(t, store.Close(ctx))
	store = open()
	t.Run("reopened", check)
}

func countStreamingInputs(base int64) []countStreamingInput {
	var inputs []countStreamingInput
	for host := range 8 {
		for cpu := range 6 {
			for mode := range 2 {
				input := countStreamingInput{host: "host-" + strconv.Itoa(host), cpu: strconv.Itoa(cpu)}
				for sample := range 80 {
					elapsed := int64(sample*15 + 7)
					active := host != 0 || countStreamingActive(cpu, elapsed)
					if mode == 1 && cpu%2 == 1 {
						active = active && sample%2 == 0
					}
					if active {
						input.samples = append(input.samples, prwpb.Sample{Timestamp: (base + elapsed) * 1000, Value: float64(sample + 1)})
					}
				}
				inputs = append(inputs, input)
			}
		}
	}
	return inputs
}

func countStreamingActive(cpu int, elapsed int64) bool {
	switch cpu {
	case 0:
		return true
	case 1:
		return elapsed < 360
	case 2:
		return elapsed >= 360 && elapsed < 720
	case 3:
		return elapsed >= 780
	case 4:
		return (elapsed-7)%420 == 0
	default:
		return elapsed < 180 || elapsed >= 1020
	}
}

func countStreamingExpected(inputs []countStreamingInput, host string, at int64) float64 {
	present := make(map[string]bool)
	for _, input := range inputs {
		if input.host != host {
			continue
		}
		for _, sample := range input.samples {
			ts := sample.Timestamp / 1000
			if ts > at-300 && ts <= at {
				present[input.cpu] = true
				break
			}
		}
	}
	return float64(len(present))
}
