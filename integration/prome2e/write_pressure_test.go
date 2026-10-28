package prome2e_test

import (
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"

	"github.com/oteldb/oteldb/internal/prompb"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/prometheusremotewrite"
)

func pressureShapes(tb testing.TB, n, names int) (fragmented, grouped pmetric.Metrics) {
	tb.Helper()
	series := make([]prompb.TimeSeries, n)
	for i := range series {
		series[i] = prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: []byte("__name__"), Value: []byte("node_probe_" + strconv.Itoa(i%names) + "_seconds_total")},
				{Name: []byte("instance"), Value: []byte("host-" + strconv.Itoa(i))},
				{Name: []byte("mode"), Value: []byte("user")},
			},
			Samples: []prompb.Sample{{Timestamp: 2_000_000_000, Value: float64(i)}},
		}
	}
	fragmented, err := prometheusremotewrite.FromTimeSeries(series, prometheusremotewrite.Settings{TimeThreshold: math.MaxInt64})
	require.NoError(tb, err)
	grouped = pmetric.NewMetrics()
	srcResource := fragmented.ResourceMetrics().At(0)
	dstResource := grouped.ResourceMetrics().AppendEmpty()
	srcResource.Resource().CopyTo(dstResource.Resource())
	dstResource.SetSchemaUrl(srcResource.SchemaUrl())
	srcScope := srcResource.ScopeMetrics().At(0)
	dstScope := dstResource.ScopeMetrics().AppendEmpty()
	srcScope.Scope().CopyTo(dstScope.Scope())
	dstScope.SetSchemaUrl(srcScope.SchemaUrl())
	metrics := srcScope.Metrics()
	byName := make(map[string]pmetric.Metric)
	for i := range metrics.Len() {
		source := metrics.At(i)
		target, ok := byName[source.Name()]
		if !ok {
			target = dstScope.Metrics().AppendEmpty()
			source.CopyTo(target)
			byName[source.Name()] = target
			continue
		}
		source.Sum().DataPoints().At(0).CopyTo(target.Sum().DataPoints().AppendEmpty())
	}
	return fragmented, grouped
}

func pressureStore(tb testing.TB, dir string, provider *sdkmetric.MeterProvider) *storage.Storage {
	tb.Helper()
	be, err := file.New(filepath.Join(dir, "parts"))
	require.NoError(tb, err)
	opts := storage.Options{Backend: be, WALDir: filepath.Join(dir, "wal"), FlushInterval: int64(time.Hour), FlushThresholdBytes: -1}
	var extra []storage.Option
	if provider != nil {
		extra = append(extra, storage.WithMeterProvider(provider))
	}
	store, err := storage.Open(tb.Context(), opts, extra...)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, store.Close(tb.Context())) })
	return store
}

func TestWritePressureShapeEquivalence(t *testing.T) {
	t.Parallel()
	for _, names := range []int{1, 128} {
		t.Run(strconv.Itoa(names), func(t *testing.T) {
			t.Parallel()
			checkPressureEquivalence(t, names)
		})
	}
}

type pressureSample struct {
	ts    int64
	value float64
}

func checkPressureEquivalence(t *testing.T, names int) {
	t.Helper()
	fragmented, grouped := pressureShapes(t, 1024, names)
	var previous []byte
	var previousIDs []signal.SeriesID
	var previousSamples map[signal.SeriesID][]pressureSample
	for _, shape := range []struct {
		name string
		data pmetric.Metrics
	}{{name: "remote-write", data: fragmented}, {name: "grouped", data: grouped}} {
		dir := t.TempDir()
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		t.Cleanup(func() { require.NoError(t, provider.Shutdown(t.Context())) })
		store := pressureStore(t, dir, provider)
		require.NoError(t, storagebackend.New(store).ConsumeMetrics(t.Context(), shape.data))
		identities, err := store.MetricSeries(t.Context(), "default", nil, 0, math.MaxInt64)
		require.NoError(t, err)
		require.Len(t, identities, 1024)
		ids := make([]signal.SeriesID, len(identities))
		for i := range identities {
			ids[i] = identities[i].Hash()
		}
		var frames []byte
		require.NoError(t, filepath.WalkDir(filepath.Join(dir, "wal"), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".wal") {
				return err
			}
			data, err := os.ReadFile(path)
			frames = append(frames, data...)
			return err
		}))
		var metrics metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &metrics))
		var appends int64
		for _, scope := range metrics.ScopeMetrics {
			for _, metric := range scope.Metrics {
				if metric.Name == "storage.wal.appends" {
					for _, point := range metric.Data.(metricdata.Sum[int64]).DataPoints {
						appends += point.Value
					}
				}
			}
		}
		samples := make(map[signal.SeriesID][]pressureSample)
		require.NoError(t, wal.Replay(frames, wal.Handlers{OnSamples: func(id signal.SeriesID, ts []int64, values []float64) error {
			for i := range ts {
				samples[id] = append(samples[id], pressureSample{ts: ts[i], value: values[i]})
			}
			return nil
		}}))
		t.Logf("names=%d shape=%s points=1024 WAL_appends=%d WAL_bytes=%d", names, shape.name, appends, len(frames))
		if previous != nil {
			if names == 1 {
				require.Equal(t, previous, frames, "homogeneous grouping preserves the byte stream")
			}
			require.Len(t, frames, len(previous))
			require.ElementsMatch(t, previousIDs, ids)
			require.Equal(t, previousSamples, samples, "grouping must preserve replayed samples")
		}
		previous, previousIDs, previousSamples = frames, ids, samples
	}
}

func BenchmarkWritePressureShape(b *testing.B) {
	const points = 1024
	for _, names := range []int{1, 128} {
		fragmented, grouped := pressureShapes(b, points, names)
		for _, shape := range []struct {
			name string
			data pmetric.Metrics
		}{{name: "remote-write", data: fragmented}, {name: "grouped", data: grouped}} {
			for _, durable := range []bool{false, true} {
				b.Run("names="+strconv.Itoa(names)+"/shape="+shape.name+"/WAL="+strconv.FormatBool(durable), func(b *testing.B) {
					var store *storage.Storage
					if durable {
						store = pressureStore(b, b.TempDir(), nil)
					} else {
						var err error
						store, err = storage.InMemory()
						require.NoError(b, err)
						b.Cleanup(func() { require.NoError(b, store.Close(b.Context())) })
					}
					backend := storagebackend.New(store)
					b.ReportAllocs()
					b.SetBytes(points * engine.SampleBytes)
					for b.Loop() {
						if err := backend.ConsumeMetrics(b.Context(), shape.data); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(points*b.N)/b.Elapsed().Seconds(), "points/s")
				})
			}
		}
	}
}
