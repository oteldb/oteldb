package prome2e_test

import (
	"context"
	"fmt"
	"math"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/queryapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestClassicHistogramNativeHTTP(t *testing.T) {
	for _, state := range []string{"head", "parts"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			store, err := storage.InMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(t.Context()))) })
			backend := storagebackend.New(store)
			testClassicHistogramHTTP(t, backend, backend, func() {
				if state == "parts" {
					require.NoError(t, store.Admin().Flush(t.Context(), "", signal.Metric))
					stats := store.Inspect()
					require.Len(t, stats.Tenants, 1)
					require.Len(t, stats.Tenants[0].Signals, 1)
					require.Positive(t, stats.Tenants[0].Signals[0].Parts)
					require.Zero(t, stats.Tenants[0].Signals[0].HeadItems)
				}
			})
		})
	}
}

func TestClassicHistogramClickHouseHTTP(t *testing.T) {
	integration.Skip(t)
	prefix := strings.ReplaceAll(uuid.NewString(), "-", "")
	_, client, tables := integration.SetupCH(t, integration.SetupCHOptions{Name: "classic-histogram-" + prefix, TablePrefix: prefix})
	inserter, err := chstorage.NewInserter(client, chstorage.InserterOptions{Tables: tables})
	require.NoError(t, err)
	querier, err := chstorage.NewQuerier(client, chstorage.QuerierOptions{Tables: tables, MetricsCacheOptions: chstorage.MetricsCacheOptions{MaxBytes: 64 * 1024 * 1024, SafetyLag: time.Minute}})
	require.NoError(t, err)
	testClassicHistogramHTTP(t, inserter, querier, nil)
}

func testClassicHistogramHTTP(t *testing.T, consumer MetricsConsumer, querier queryapi.MetricQuerier, prepare func()) {
	t.Helper()
	ctx := t.Context()
	base := time.Unix(2_000_000, 0).UTC()
	md := pmetric.NewMetrics()
	resource := md.ResourceMetrics().AppendEmpty()
	resource.Resource().Attributes().PutStr("service.name", "classic-histogram-test")
	metrics := resource.ScopeMetrics().AppendEmpty().Metrics()
	metric := metrics.AppendEmpty()
	metric.SetName("classic_duration")
	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	for i := range 6 {
		point := histogram.DataPoints().AppendEmpty()
		point.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(i-3) * 30 * time.Second).UnixNano()))
		point.ExplicitBounds().FromRaw([]float64{1, 2})
		point.BucketCounts().FromRaw([]uint64{uint64(10 * (i + 1)), uint64(10 * (i + 1)), uint64(20 * (i + 1))})
		point.SetCount(uint64(40 * (i + 1)))
		point.SetSum(float64(70 * (i + 1)))
	}
	manual := metrics.AppendEmpty()
	manual.SetName("classic_manual_bucket")
	gauge := manual.SetEmptyGauge()
	for i, bound := range []string{"1", "2", "+Inf"} {
		point := gauge.DataPoints().AppendEmpty()
		point.SetTimestamp(pcommon.Timestamp(base.UnixNano()))
		point.SetDoubleValue([]float64{10, 5, 20}[i])
		point.Attributes().PutStr("le", bound)
	}
	require.NoError(t, consumer.ConsumeMetrics(ctx, md))
	if prepare != nil {
		prepare()
	}
	server, err := queryapi.NewPrometheus(queryapi.PrometheusOptions{Querier: querier})
	require.NoError(t, err)
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	client, err := promapi.NewClient(httpServer.URL, promapi.WithClient(httpServer.Client()))
	require.NoError(t, err)
	params := func(query string) promapi.GetQueryRangeParams {
		return promapi.GetQueryRangeParams{Query: query, Start: getPromTS(pcommon.Timestamp(base.UnixNano())), End: getPromTS(pcommon.Timestamp(base.Add(time.Minute).UnixNano())), Step: promapi.NewOptString("30s")}
	}
	for _, query := range []string{
		`histogram_quantile(0.5, classic_duration_bucket)`,
		`histogram_quantile(0.5, sum by(le)(rate(classic_duration_bucket[1m])))`,
	} {
		t.Run(query, func(t *testing.T) {
			response, err := client.GetQueryRange(ctx, params(query))
			require.NoError(t, err)
			require.Equal(t, promapi.MatrixData, response.Data.Type)
			require.Len(t, response.Data.Matrix.Result, 1)
			series := response.Data.Matrix.Result[0]
			require.NotContains(t, series.Metric, "le")
			require.NotContains(t, series.Metric, "__name__")
			require.Len(t, series.Values, 3)
			for _, point := range series.Values {
				require.InDelta(t, 2.0, point.V, 1e-12)
			}
		})
	}
	t.Run("ScalarPerStep", func(t *testing.T) {
		response, err := client.GetQueryRange(ctx, params(fmt.Sprintf(`histogram_quantile((time()-%d)/60, classic_duration_bucket)`, base.Unix())))
		require.NoError(t, err)
		require.Len(t, response.Data.Matrix.Result, 1)
		for i, point := range response.Data.Matrix.Result[0].Values {
			require.InDelta(t, []float64{0, 2, 2}[i], point.V, 1e-12)
		}
	})
	t.Run("Warnings", func(t *testing.T) {
		response, err := client.GetQueryRange(ctx, params(`histogram_quantile(-1, classic_duration_bucket)`))
		require.NoError(t, err)
		require.Len(t, response.Warnings, 1)
		require.Contains(t, response.Warnings[0], "quantile value should be between 0 and 1")
		for _, point := range response.Data.Matrix.Result[0].Values {
			require.True(t, math.IsInf(point.V, -1))
		}
	})
	t.Run("RepairInfo", func(t *testing.T) {
		response, err := client.GetQueryRange(ctx, params(`histogram_quantile(0.5, classic_manual_bucket)`))
		require.NoError(t, err)
		require.Len(t, response.Infos, 1)
		require.Contains(t, response.Infos[0], "over 3 samples from")
	})
}

func BenchmarkClassicHistogramNative(b *testing.B) {
	ctx := context.Background()
	store, err := storage.InMemory()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(ctx); err != nil {
			b.Fatal(err)
		}
	})
	backend := storagebackend.New(store)
	md := pmetric.NewMetrics()
	metric := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("native_duration")
	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	bounds := make([]float64, 11)
	for i := range bounds {
		bounds[i] = float64(i + 1)
	}
	base := time.Unix(2_000_000, 0)
	for group := range 100 {
		for step := range 241 {
			point := histogram.DataPoints().AppendEmpty()
			point.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(step) * 30 * time.Second).UnixNano()))
			point.Attributes().PutStr("job", strconv.Itoa(group))
			point.ExplicitBounds().FromRaw(bounds)
			for range 12 {
				point.BucketCounts().Append(uint64(step + 1))
			}
			point.SetCount(uint64(12 * (step + 1)))
			point.SetSum(float64(80 * (step + 1)))
		}
	}
	if err := backend.ConsumeMetrics(ctx, md); err != nil {
		b.Fatal(err)
	}
	engine, err := queryapi.NewPromEngine(queryapi.PrometheusOptions{Querier: backend})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		query, err := engine.NewRangeQuery(ctx, backend, nil, "histogram_quantile(0.9, native_duration_bucket)", base, base.Add(7200*time.Second), 30*time.Second)
		if err != nil {
			b.Fatal(err)
		}
		result := query.Exec(ctx)
		if result.Err != nil {
			b.Fatal(result.Err)
		}
		if len(result.Value.(promql.Matrix)) != 100 {
			b.Fatal("missing histograms")
		}
		query.Close()
	}
}
