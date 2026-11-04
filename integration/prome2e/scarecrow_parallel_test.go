package prome2e_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/queryapi"
	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

type parallelFileQuerier struct {
	*storagebackend.Backend
	partitions atomic.Int64
}

func (q *parallelFileQuerier) ScarecrowScanner() scarecrow.Scanner {
	scanner := q.Backend.ScarecrowScanner()
	return &parallelFileScanner{Scanner: scanner, owner: q, barrier: make(chan struct{})}
}

type parallelFileScanner struct {
	scarecrow.Scanner
	owner   *parallelFileQuerier
	entered atomic.Int32
	barrier chan struct{}
}

func (s *parallelFileScanner) WithQueryBudget(ctx context.Context) context.Context {
	return s.Scanner.(scarecrow.QueryBudgetScanner).WithQueryBudget(ctx)
}

func (s *parallelFileScanner) ScanPartitions(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher, workers int) ([]scarecrow.PartitionIterator, error) {
	parts, err := s.Scanner.(scarecrow.PartitionScanner).ScanPartitions(ctx, mint, maxt, matchers, workers)
	if err != nil {
		return nil, err
	}
	s.owner.partitions.Add(int64(len(parts)))
	for i, part := range parts {
		parts[i] = &parallelFileIterator{PartitionIterator: part, scanner: s}
	}
	return parts, nil
}

type parallelFileIterator struct {
	scarecrow.PartitionIterator
	scanner *parallelFileScanner
}

func (it *parallelFileIterator) Next(ctx context.Context) (*scarecrow.Samples, error) {
	samples, err := it.PartitionIterator.Next(ctx)
	if err != nil || samples == nil {
		return samples, err
	}
	// Both workers must own a different native batch before either can fold. This proves actual
	// overlap and that advancing the shared file iterator preserves another worker's borrow.
	switch it.scanner.entered.Add(1) {
	case 1:
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-it.scanner.barrier:
		}
	case 2:
		close(it.scanner.barrier)
	}
	return samples, nil
}

func TestScarecrowParallelFoldNativeFileHTTP(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("native fold overlap requires at least two process concurrency slots")
	}
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, state := range []string{"head", "reopened-parts"} {
		t.Run(state, func(t *testing.T) {
			dir := t.TempDir()
			files, err := backendfile.New(filepath.Join(dir, "parts"))
			require.NoError(t, err)
			store, err := storage.Open(t.Context(), storage.Options{}, storage.WithBackend(files),
				storage.WithWALDir(filepath.Join(dir, "wal")), storage.WithFlushInterval(-1))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(t.Context()))) })
			backend := storagebackend.New(store)
			base := time.Unix(2_000_000, 0).UTC()
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			metric.SetName("parallel_counter")
			points := metric.SetEmptyGauge().DataPoints()
			for i := range 8 {
				for j := range 121 {
					point := points.AppendEmpty()
					point.Attributes().PutStr("instance", strconv.Itoa(i))
					point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(j) * 15 * time.Second)))
					point.SetDoubleValue(float64(j * (i + 1)))
				}
			}
			require.NoError(t, backend.ConsumeMetrics(t.Context(), metrics))
			if state == "reopened-parts" {
				require.NoError(t, store.Admin().Flush(t.Context(), "", signal.Metric))
				require.NoError(t, store.Close(t.Context()))
				store, err = storage.Open(t.Context(), storage.Options{}, storage.WithBackend(files),
					storage.WithWALDir(filepath.Join(dir, "wal")), storage.WithFlushInterval(-1))
				require.NoError(t, err)
				backend = storagebackend.New(store)
			}
			querier := &parallelFileQuerier{Backend: backend}
			handler, err := queryapi.NewPrometheus(queryapi.PrometheusOptions{Querier: querier})
			require.NoError(t, err)
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)
			client, err := promapi.NewClient(server.URL, promapi.WithClient(server.Client()))
			require.NoError(t, err)
			for _, tt := range []struct {
				query string
				want  float64
			}{
				{`avg(rate(parallel_counter[1m]))`, 0.3},
				{`avg(predict_linear(parallel_counter[1m], time())) / (time()*0.3 + (time()-2000000)*0.3)`, 1},
				{`avg(rate(parallel_counter[1m])) / avg(rate(parallel_counter[1m]))`, 1},
			} {
				before := querier.partitions.Load()
				response, err := client.GetQueryRange(t.Context(), promapi.GetQueryRangeParams{
					Query: tt.query,
					Start: getPromTS(pcommon.NewTimestampFromTime(base.Add(15 * time.Minute))),
					End:   getPromTS(pcommon.NewTimestampFromTime(base.Add(30 * time.Minute))),
					Step:  promapi.NewOptString("15s"),
				})
				require.NoError(t, err)
				require.Greater(t, querier.partitions.Load(), before, tt.query)
				require.Equal(t, promapi.MatrixData, response.Data.Type)
				require.Len(t, response.Data.Matrix.Result, 1)
				require.Len(t, response.Data.Matrix.Result[0].Values, 61)
				for _, point := range response.Data.Matrix.Result[0].Values {
					require.InDelta(t, tt.want, point.V, 1e-12)
				}
			}
			require.GreaterOrEqual(t, querier.partitions.Load(), int64(2))
		})
	}
}
