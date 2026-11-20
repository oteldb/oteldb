//go:build headbench

package prome2e_test

import (
	"context"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/go-faster/sdk/app"
	"github.com/go-faster/yaml"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/oteldb/storage/signal"
	sigmetric "github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestHeadCountGridAdapter(t *testing.T) {
	ctx := t.Context()
	var cfg storagebackend.Config
	require.NoError(t, yaml.Unmarshal([]byte("backend: file\nmetric_disk_series: true\nmetric_series_chunks: true\nmetric_resident_head_bytes: 128MiB\nmetric_series_cache_bytes: 512MiB\nread_cache_bytes: 0\nnative_query_cache_bytes: 0\n"), &cfg))
	cfg.Dir = t.TempDir()
	cfg.FlushInterval = time.Hour
	backend, closeStore, err := storagebackend.Open(ctx, cfg, zap.NewNop(), &app.Telemetry{})
	require.NoError(t, err)
	stop, err := storagebackend.EnableHeadExperiment(ctx, backend, 8, zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { stop(); require.NoError(t, closeStore(context.WithoutCancel(ctx))) })
	base := time.Unix(1700000000, 0)
	write := func(host int, at time.Time, v float64) {
		var md sigmetric.Metrics
		m := md.AddResource().AddScope().AddMetric()
		m.Name = []byte("head_count_metric")
		p := m.AddPoint()
		p.Ts = at.UnixNano()
		p.Value = v
		p.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(strconv.Itoa(host)))}, signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("job-" + strconv.Itoa(host%2)))})
		require.NoError(t, backend.WriteMetrics(ctx, md))
	}
	for host := range 32 {
		write(host, base.Add(-time.Minute), 1)
		write(host, base, 1)
		write(host, base, 1)
	}
	write(99, base.Add(-5*time.Minute), 1)
	optimized := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(promstorage.Queryable) scarecrow.Scanner { return backend.ScarecrowScanner() }})
	ordinary := scarecrow.NewEngine(scarecrow.Opts{})
	check := func(at time.Time) {
		for _, expr := range []string{"count(head_count_metric)", "count by(job)(head_count_metric)", "count(count by(job)(head_count_metric))"} {
			want, err := ordinary.NewInstantQuery(ctx, backend, nil, expr, at)
			require.NoError(t, err)
			got, err := optimized.NewInstantQuery(ctx, backend, nil, expr, at)
			require.NoError(t, err)
			w, g := want.Exec(ctx), got.Exec(ctx)
			require.NoError(t, w.Err)
			require.NoError(t, g.Err)
			require.Equal(t, headCountValue(w.Value), headCountValue(g.Value), expr)
			want.Close()
			got.Close()
			wr, err := ordinary.NewRangeQuery(ctx, backend, nil, expr, at.Add(-time.Second), at.Add(2*time.Second), time.Second)
			require.NoError(t, err)
			gr, err := optimized.NewRangeQuery(ctx, backend, nil, expr, at.Add(-time.Second), at.Add(2*time.Second), time.Second)
			require.NoError(t, err)
			w, g = wr.Exec(ctx), gr.Exec(ctx)
			require.NoError(t, w.Err)
			require.NoError(t, g.Err)
			require.Equal(t, headCountValue(w.Value), headCountValue(g.Value), expr)
			wr.Close()
			gr.Close()
		}
	}
	scanner := backend.ScarecrowScanner()
	_, ok := scanner.(scarecrow.SeriesCounter)
	require.True(t, ok)
	require.NoError(t, scanner.Close())
	check(base)
	for lane := range 8 {
		require.NoError(t, backend.Store().Admin().Flush(ctx, signal.TenantID("default/_s"+strconv.Itoa(lane)), signal.Metric))
	}
	check(base)
	write(0, base.Add(time.Second), math.Float64frombits(value.StaleNaN))
	scanner = backend.ScarecrowScanner()
	_, ok = scanner.(scarecrow.SeriesCounter)
	require.False(t, ok)
	_, ok = scanner.(scarecrow.GridAggregateScanner)
	require.False(t, ok)
	require.NoError(t, scanner.Close())
	check(base.Add(time.Second))
	write(0, base.Add(2*time.Second), 1)
	check(base.Add(2 * time.Second))
}

func headCountValue(v parser.Value) string {
	switch values := v.(type) {
	case promql.Vector:
		copy := slices.Clone(values)
		slices.SortFunc(copy, func(a, b promql.Sample) int { return labels.Compare(a.Metric, b.Metric) })
		return copy.String()
	case promql.Matrix:
		copy := slices.Clone(values)
		slices.SortFunc(copy, func(a, b promql.Series) int { return labels.Compare(a.Metric, b.Metric) })
		return copy.String()
	default:
		return v.String()
	}
}

// BenchmarkHeadCountProbe isolates the live probe over eight file-backed lanes.
func BenchmarkHeadCountProbe(b *testing.B) {
	ctx := b.Context()
	var cfg storagebackend.Config
	if err := yaml.Unmarshal([]byte("backend: file\nmetric_disk_series: true\nmetric_series_chunks: true\nmetric_resident_head_bytes: 128MiB\nmetric_series_cache_bytes: 512MiB\nread_cache_bytes: 0\nnative_query_cache_bytes: 0\n"), &cfg); err != nil {
		b.Fatal(err)
	}
	cfg.Dir = b.TempDir()
	cfg.FlushInterval = time.Hour
	backend, closeStore, err := storagebackend.Open(ctx, cfg, zap.NewNop(), &app.Telemetry{})
	if err != nil {
		b.Fatal(err)
	}
	stop, err := storagebackend.EnableHeadExperiment(ctx, backend, 8, zap.NewNop())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		stop()
		if err := closeStore(context.WithoutCancel(ctx)); err != nil {
			b.Error(err)
		}
	})
	const hosts = 10000
	base := time.Unix(1700000000, 0)
	var md sigmetric.Metrics
	scope := md.AddResource().AddScope()
	for metric := range 16 {
		m := scope.AddMetric()
		m.Name = []byte("bench_other_" + strconv.Itoa(metric))
		if metric == 0 {
			m.Name = []byte("node_uname_info")
		}
		for host := range hosts {
			p := m.AddPoint()
			p.Ts = base.UnixNano()
			p.Value = 1
			p.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(strconv.Itoa(host)))})
		}
	}
	if err := backend.WriteMetrics(ctx, md); err != nil {
		b.Fatal(err)
	}
	for lane := range 8 {
		if err := backend.Store().Admin().Flush(ctx, signal.TenantID("default/_s"+strconv.Itoa(lane)), signal.Metric); err != nil {
			b.Fatal(err)
		}
	}
	evaluator := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(promstorage.Queryable) scarecrow.Scanner { return backend.ScarecrowScanner() }})
	b.ReportAllocs()
	for b.Loop() {
		query, err := evaluator.NewInstantQuery(ctx, backend, nil, `count(node_uname_info)`, base)
		if err != nil {
			b.Fatal(err)
		}
		result := query.Exec(ctx)
		if result.Err != nil {
			query.Close()
			b.Fatal(result.Err)
		}
		vector, ok := result.Value.(promql.Vector)
		if !ok || len(vector) != 1 || vector[0].F != hosts {
			query.Close()
			b.Fatalf("incorrect count: %v", result.Value)
		}
		query.Close()
	}
}
