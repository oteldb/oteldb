package storagebackend_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/queryapi"
	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

type nativeQueryFixture struct {
	Version           int    `json:"version"`
	PartsDir          string `json:"parts_dir"`
	Hosts             int    `json:"hosts"`
	ExpectedSeries    int    `json:"expected_series"`
	End               int64  `json:"end_unix"`
	TargetsFile       string `json:"targets_file"`
	ReadCacheBytes    int64  `json:"read_cache_bytes"`
	DecodeCacheBytes  int64  `json:"decode_cache_bytes"`
	DecodeMemoryBytes int64  `json:"decode_memory_bytes"`
	MaxQueryBytes     int64  `json:"max_query_bytes"`
}

type nativeQueryTarget struct {
	Query   string `json:"query"`
	Instant bool   `json:"instant"`
	Index   int    `json:"query_index"`
}

func BenchmarkNativeDashboard10K(b *testing.B) {
	dir := os.Getenv("OTELDB_NATIVE_QUERY_FIXTURE")
	if dir == "" {
		b.Skip("set OTELDB_NATIVE_QUERY_FIXTURE to an immutable native-file fixture")
	}
	var fixture nativeQueryFixture
	readNativeFixtureJSON(b, filepath.Join(dir, "manifest.json"), &fixture)
	require.Equal(b, 1, fixture.Version)
	require.Equal(b, 10000, fixture.Hosts)
	require.True(b, filepath.IsLocal(fixture.PartsDir))
	require.True(b, filepath.IsLocal(fixture.TargetsFile))
	var targets []nativeQueryTarget
	readNativeFixtureJSON(b, filepath.Join(dir, fixture.TargetsFile), &targets)
	require.Len(b, targets, 242)
	files, err := backendfile.New(filepath.Join(dir, fixture.PartsDir))
	require.NoError(b, err)
	b.Log("opening immutable native fixture")
	opened := time.Now()
	store, err := storage.Open(b.Context(), storage.Options{
		Backend: files, ReadOnly: true,
		ReadCacheBytes: fixture.ReadCacheBytes, DecodeCacheBytes: fixture.DecodeCacheBytes,
		DecodeMemoryBytes: fixture.DecodeMemoryBytes, MaxQueryBytes: fixture.MaxQueryBytes,
	})
	require.NoError(b, err)
	b.Logf("loaded fixture in %s", time.Since(opened))
	b.Cleanup(func() { require.NoError(b, store.Close(context.WithoutCancel(b.Context()))) })
	stats := store.Inspect()
	require.Len(b, stats.Tenants, 1)
	require.Len(b, stats.Tenants[0].Signals, 1)
	metricStats := stats.Tenants[0].Signals[0]
	require.Equal(b, signal.Metric, metricStats.Signal)
	require.EqualValues(b, fixture.ExpectedSeries, metricStats.Series)
	backend := storagebackend.New(store)
	engine := queryapi.NewScarecrowEngine(nil, backend, config.Prometheus{}, noop.NewTracerProvider())
	for _, gate := range []struct {
		query string
		want  float64
	}{
		{`count(node_uname_info{job="node_exporter"})`, float64(fixture.Hosts)},
		{`min(count_over_time(node_uname_info{job="node_exporter"}[120s]))`, 8},
	} {
		query, err := engine.NewInstantQuery(b.Context(), nil, nil, gate.query, time.Unix(fixture.End, 0))
		require.NoError(b, err)
		result := query.Exec(b.Context())
		require.NoError(b, result.Err)
		vector, err := result.Vector()
		require.NoError(b, err)
		require.Len(b, vector, 1)
		require.Equal(b, gate.want, vector[0].F)
		query.Close()
	}
	b.Logf("verified %d hosts, %d series, %d parts at %s", fixture.Hosts,
		fixture.ExpectedSeries, metricStats.Parts, time.Unix(fixture.End, 0).UTC())
	if selected := os.Getenv("OTELDB_NATIVE_QUERY_TARGET"); selected != "" {
		index, err := strconv.Atoi(selected)
		require.NoError(b, err)
		found := false
		for _, target := range targets {
			if target.Index == index {
				targets = []nativeQueryTarget{target}
				found = true
				break
			}
		}
		require.True(b, found, "target index must exist")
	}
	for _, width := range []int64{60, 600} {
		for _, mode := range []string{"random", "repeated"} {
			b.Run(fmt.Sprintf("range=%ds/%s/clients=4", width, mode), func(b *testing.B) {
				ctx := b.Context()
				require.NoError(b, executeNativeBoard(ctx, engine, targets, "host-0", fixture.End, width))
				stopProfile := startNativeQueryProfiles(b)
				defer stopProfile()
				random := rand.New(rand.NewPCG(20261027, 10000))
				b.ReportAllocs()
				for b.Loop() {
					host := "host-0"
					if mode == "random" {
						host = "host-" + strconv.Itoa(1+random.IntN(fixture.Hosts-1))
					}
					if err := executeNativeBoard(ctx, engine, targets, host, fixture.End, width); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(targets)), "queries/op")
			})
		}
	}
}

func readNativeFixtureJSON(tb testing.TB, path string, dst any) {
	tb.Helper()
	data, err := os.ReadFile(path)
	require.NoError(tb, err)
	require.NoError(tb, json.Unmarshal(data, dst))
}

func executeNativeBoard(ctx context.Context, engine *scarecrow.Engine, targets []nativeQueryTarget, host string, end, width int64) error {
	var next atomic.Int64
	var wg sync.WaitGroup
	var errs [4]error
	for worker := range errs {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= len(targets) {
					return
				}
				target := targets[i]
				expr := strings.ReplaceAll(target.Query, "host-0", host)
				var query promql.Query
				var err error
				if target.Instant {
					query, err = engine.NewInstantQuery(ctx, nil, nil, expr, time.Unix(end, 0))
				} else {
					query, err = engine.NewRangeQuery(ctx, nil, nil, expr,
						time.Unix(end-width, 0), time.Unix(end, 0), 15*time.Second)
				}
				if err != nil {
					errs[worker] = errors.Wrapf(err, "target %d", target.Index)
					return
				}
				result := query.Exec(ctx)
				query.Close()
				if result.Err != nil {
					errs[worker] = errors.Wrapf(result.Err, "target %d", target.Index)
					return
				}
			}
		})
	}
	wg.Wait()
	return errors.Join(errs[:]...)
}

func startNativeQueryProfiles(b *testing.B) func() {
	b.Helper()
	dir := os.Getenv("OTELDB_NATIVE_QUERY_PROFILE_DIR")
	if dir == "" {
		return func() {}
	}
	require.NoError(b, os.MkdirAll(dir, 0o750))
	name := strings.NewReplacer("/", "-", "=", "-").Replace(b.Name())
	dir, err := os.MkdirTemp(dir, name+"-")
	require.NoError(b, err)
	write := func(name string) {
		file, err := os.Create(filepath.Join(dir, name))
		require.NoError(b, err)
		require.NoError(b, pprof.Lookup("allocs").WriteTo(file, 0))
		require.NoError(b, file.Close())
	}
	runtime.GC()
	runtime.GC()
	write("allocs-before.pb.gz")
	file, err := os.Create(filepath.Join(dir, "cpu.pb.gz"))
	require.NoError(b, err)
	require.NoError(b, pprof.StartCPUProfile(file))
	b.Logf("query-only profiles: %s", dir)
	return func() {
		pprof.StopCPUProfile()
		require.NoError(b, file.Close())
		runtime.GC()
		runtime.GC()
		write("allocs-after.pb.gz")
	}
}
