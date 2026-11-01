package prome2e_test

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestStorageFileReadsAndWritesDuringPublication(t *testing.T) {
	t.Parallel()
	integration.Skip(t)
	for _, points := range []int{1, 4097} {
		t.Run(strconv.Itoa(points), func(t *testing.T) {
			testStorageFileReadsAndWritesDuringPublication(t, points)
		})
	}
}

func testStorageFileReadsAndWritesDuringPublication(t *testing.T, points int) {
	t.Helper()
	ctx := t.Context()
	dir := t.TempDir()
	files, err := file.New(filepath.Join(dir, "parts"))
	require.NoError(t, err)
	be := faultbackend.Wrap(files)
	opts := storage.Options{
		Backend: be, WALDir: filepath.Join(dir, "wal"),
		FlushInterval: int64(time.Hour), FlushThresholdBytes: -1,
	}
	store, err := storage.Open(ctx, opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	t.Cleanup(release)
	base := time.Unix(2_000_000, 0)
	queryAt := base.Add(time.Duration(2*points)*time.Millisecond + time.Second).Truncate(time.Second)
	write := func(round int) error {
		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("publication_probe")
		dps := m.SetEmptyGauge().DataPoints()
		for point := range points {
			p := dps.AppendEmpty()
			p.Attributes().PutStr("instance", "host-0")
			p.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(round*points+point) * time.Millisecond)))
			p.SetDoubleValue(float64(round + 1))
		}
		return storagebackend.New(store).ConsumeMetrics(ctx, md)
	}
	require.NoError(t, write(0))
	_, client := setupDB(t, integration.TraceProvider(t), storagebackend.New(store))
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, "/"+bucketindex.Object)
	}))
	flushed := make(chan error, 1)
	go func() { flushed <- store.Admin().Flush(ctx, "", signal.Metric) }()
	gate.Await(t)
	written := make(chan error, 1)
	go func() { written <- write(1) }()
	select {
	case err := <-written:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("write blocked behind bucket-index publication")
	}
	check := func(client *promapi.Client) {
		t.Helper()
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		response, err := client.GetQuery(readCtx, promapi.GetQueryParams{
			Query: `count_over_time(publication_probe{instance="host-0"}[1m])`,
			Time:  promapi.NewOptPrometheusTimestamp(getPromTS(pcommon.NewTimestampFromTime(queryAt))),
		})
		require.NoError(t, err)
		require.Len(t, response.Data.Vector.Result, 1)
		value, ok := response.Data.Vector.Result[0].Value.HistogramOrValue.GetStringFloat64()
		require.True(t, ok)
		require.InDelta(t, float64(2*points), float64(value), 0)
	}
	check(client)
	release()
	require.NoError(t, <-flushed)
	check(client)
	require.NoError(t, store.Close(ctx))
	reopened, err := storage.Open(ctx, opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close(context.Background())) })
	_, reopenedClient := setupDB(t, integration.TraceProvider(t), storagebackend.New(reopened))
	check(reopenedClient)
}
