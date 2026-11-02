package prome2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
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

func TestNativeMergeBudgetFileWAL(t *testing.T) {
	t.Parallel()
	const (
		base        = int64(1700000000)
		buildBudget = 64 << 20
		hosts       = 8192
	)
	ctx := t.Context()
	dir := t.TempDir()
	files, err := backendfile.New(filepath.Join(dir, "parts"))
	require.NoError(t, err)
	open := func() *storage.Storage {
		store, err := storage.Open(ctx, storage.Options{MergeMemoryBytes: buildBudget}, storage.WithBackend(files),
			storage.WithWALDir(filepath.Join(dir, "wal")), storage.WithFlushInterval(-1), storage.WithFlushThresholdBytes(-1))
		require.NoError(t, err)
		return store
	}
	store := open()
	t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(ctx))) })
	handler := promrw.NewHandler(storagebackend.New(store), promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: 100 * 365 * 24 * time.Hour},
	})
	payloads := make([]string, hosts)
	prefix := strings.Repeat("x", 2048)
	for host := range payloads {
		payloads[host] = prefix + strconv.Itoa(host)
	}
	for epoch := range 3 {
		writeMergeBudgetEpoch(t, handler, payloads, base, epoch)
		require.NoError(t, store.Admin().Flush(ctx, "default", signal.Metric))
	}
	check := func(t *testing.T, epochs int) {
		t.Helper()
		checkMergeBudgetQueries(t, store, payloads, base, epochs)
		checkMergeBudgetObjects(t, files, buildBudget)
	}
	t.Run("flushed", func(t *testing.T) { check(t, 3) })
	require.NoError(t, store.Admin().CompactNow(ctx, "default", signal.Metric))
	t.Run("compacted", func(t *testing.T) { check(t, 3) })
	writeMergeBudgetEpoch(t, handler, payloads, base, 3)
	require.NoError(t, store.Close(ctx))
	store = open()
	t.Run("reopened", func(t *testing.T) { check(t, 4) })
}

func writeMergeBudgetEpoch(t *testing.T, handler http.Handler, payloads []string, base int64, epoch int) {
	t.Helper()
	for first := 0; first < len(payloads); first += 512 {
		request := prwpb.WriteRequest{}
		for host := first; host < min(first+512, len(payloads)); host++ {
			request.Timeseries = append(request.Timeseries, prwpb.TimeSeries{
				Labels: []prwpb.Label{
					{Name: "__name__", Value: "merge_budget_counter"},
					{Name: "instance", Value: "host-" + strconv.Itoa(host)},
					{Name: "payload", Value: payloads[host]},
				},
				Samples: []prwpb.Sample{{Timestamp: (base + int64(epoch)*15) * 1000, Value: float64(host*10 + epoch + 1)}},
			})
		}
		raw, err := request.Marshal()
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(snappy.Encode(nil, raw)))
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "snappy")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	}
}

func checkMergeBudgetQueries(t *testing.T, store *storage.Storage, payloads []string, base int64, epochs int) {
	t.Helper()
	ctx := t.Context()
	_, client := setupDB(t, integration.TraceProvider(t), storagebackend.New(store))
	result, err := client.GetQueryRange(ctx, promapi.GetQueryRangeParams{
		Query: `sum by(instance)(merge_budget_counter{payload!=""})`,
		Start: promapi.PrometheusTimestamp(strconv.FormatInt(base, 10)),
		End:   promapi.PrometheusTimestamp(strconv.FormatInt(base+int64(epochs-1)*15, 10)),
		Step:  promapi.NewOptString("15s"),
	})
	require.NoError(t, err)
	require.Equal(t, promapi.MatrixData, result.Data.Type)
	require.Len(t, result.Data.Matrix.Result, len(payloads))
	seen := make([]bool, len(payloads))
	for _, row := range result.Data.Matrix.Result {
		instance := row.Metric["instance"]
		require.True(t, strings.HasPrefix(instance, "host-"))
		host, err := strconv.Atoi(strings.TrimPrefix(instance, "host-"))
		require.NoError(t, err)
		require.GreaterOrEqual(t, host, 0)
		require.Less(t, host, len(payloads))
		require.False(t, seen[host])
		seen[host] = true
		require.Len(t, row.Values, epochs)
		for epoch, point := range row.Values {
			require.EqualValues(t, base+int64(epoch)*15, point.T)
			require.InDelta(t, float64(host*10+epoch+1), point.V, 0)
		}
	}
	for _, host := range []int{0, len(payloads) / 2, len(payloads) - 1} {
		response, err := client.GetQuery(ctx, promapi.GetQueryParams{
			Query: fmt.Sprintf(`merge_budget_counter{instance=%q,payload=%q}`, "host-"+strconv.Itoa(host), payloads[host]),
			Time:  promapi.NewOptPrometheusTimestamp(promapi.PrometheusTimestamp(strconv.FormatInt(base+int64(epochs-1)*15, 10))),
		})
		require.NoError(t, err)
		require.Len(t, response.Data.Vector.Result, 1)
		row := response.Data.Vector.Result[0]
		require.Equal(t, payloads[host], row.Metric["payload"])
		value, ok := row.Value.HistogramOrValue.GetStringFloat64()
		require.True(t, ok)
		require.InDelta(t, float64(host*10+epochs), float64(value), 0)
	}
}

func checkMergeBudgetObjects(t *testing.T, files *backendfile.File, budget int64) {
	t.Helper()
	keys, err := files.List(t.Context(), "default/metrics/")
	require.NoError(t, err)
	var identities int
	var largest int64
	for _, key := range keys {
		if !strings.HasSuffix(key, "/identity") {
			continue
		}
		size, err := files.Size(t.Context(), key)
		require.NoError(t, err)
		require.Positive(t, size)
		require.LessOrEqual(t, size, budget, "an encoded identity object must fit within the build allowance")
		largest = max(largest, size)
		identities++
	}
	require.Positive(t, identities)
	t.Logf("identity objects=%d largest=%d build_budget=%d", identities, largest, budget)
}
