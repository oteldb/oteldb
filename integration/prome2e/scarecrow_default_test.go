package prome2e_test

import (
	"cmp"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/queryapi"
)

func TestScarecrowClickHouseRateParity(t *testing.T) {
	integration.Skip(t)
	ctx := t.Context()
	provider := integration.TraceProvider(t)
	prefix := strings.ReplaceAll(uuid.NewString(), "-", "")
	_, client, tables := integration.SetupCH(t, integration.SetupCHOptions{
		Name: "scarecrow-rate-" + prefix, TablePrefix: prefix, TracerProvider: provider,
	})
	inserter, err := chstorage.NewInserter(client, chstorage.InserterOptions{Tables: tables, TracerProvider: provider})
	require.NoError(t, err)
	base := time.Unix(2_000_000, 0).UTC()
	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("scarecrow_counter")
	points := m.SetEmptyGauge().DataPoints()
	for _, series := range []string{"active", "old"} {
		for i := range 25 {
			if series == "old" && i > 4 {
				break
			}
			point := points.AppendEmpty()
			point.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(i-16) * 15 * time.Second).UnixNano()))
			point.SetDoubleValue(float64(i % 7))
			point.Attributes().PutStr("series", series)
			point.Attributes().PutStr("__type__", "counter")
			point.Attributes().PutStr("__unit__", "seconds")
		}
	}
	require.NoError(t, inserter.ConsumeMetrics(ctx, metrics))
	clients := make([]*promapi.Client, 3)
	for i := range clients {
		querier, err := chstorage.NewQuerier(client, chstorage.QuerierOptions{
			Tables: tables, TracerProvider: provider, DisableRateOffloading: i == 1, DisableMetricOffloading: i == 2,
		})
		require.NoError(t, err)
		server, err := queryapi.NewPrometheus(queryapi.PrometheusOptions{
			Querier: querier, Config: config.Prometheus{EnableAtModifier: true}, TracerProvider: provider,
		})
		require.NoError(t, err)
		httpServer := httptest.NewServer(server)
		t.Cleanup(httpServer.Close)
		clients[i], err = promapi.NewClient(httpServer.URL, promapi.WithClient(httpServer.Client()))
		require.NoError(t, err)
	}
	for _, fn := range []string{"rate", "increase", "delta", "irate", "idelta"} {
		for _, modifier := range []string{"", " offset 30s", " offset -30s", fmt.Sprintf(" @ %d", base.Unix())} {
			query := fn + "(scarecrow_counter[5m]" + modifier + ")"
			t.Run(query, func(t *testing.T) {
				params := promapi.GetQueryRangeParams{Query: query, Start: getPromTS(pcommon.Timestamp(base.UnixNano())), End: getPromTS(pcommon.Timestamp(base.Add(time.Minute).UnixNano())), Step: promapi.NewOptString("15s")}
				got, err := clients[0].GetQueryRange(ctx, params)
				require.NoError(t, err)
				want, err := clients[1].GetQueryRange(ctx, params)
				require.NoError(t, err)
				unoffloaded, err := clients[2].GetQueryRange(ctx, params)
				require.NoError(t, err)
				require.Equal(t, want.Data, unoffloaded.Data)
				require.Len(t, want.Data.Matrix.Result, 2, "the old series remains in the first window")
				require.Len(t, got.Data.Matrix.Result, len(want.Data.Matrix.Result))
				for i, series := range want.Data.Matrix.Result {
					require.Equal(t, series.Metric, got.Data.Matrix.Result[i].Metric)
					require.Equal(t, "counter", got.Data.Matrix.Result[i].Metric["__type__"])
					require.Len(t, got.Data.Matrix.Result[i].Values, len(series.Values))
					for j, point := range series.Values {
						require.Equal(t, point.T, got.Data.Matrix.Result[i].Values[j].T)
						require.InDelta(t, point.V, got.Data.Matrix.Result[i].Values[j].V, 1e-9)
					}
				}
			})
		}
	}
	t.Run("Instant", func(t *testing.T) {
		params := promapi.GetQueryParams{Query: "rate(scarecrow_counter[5m])", Time: promapi.NewOptPrometheusTimestamp(getPromTS(pcommon.Timestamp(base.UnixNano())))}
		got, err := clients[0].GetQuery(ctx, params)
		require.NoError(t, err)
		want, err := clients[1].GetQuery(ctx, params)
		require.NoError(t, err)
		require.Equal(t, want.Data.Type, got.Data.Type)
		sortVector := func(points []promapi.VectorResultItem) {
			slices.SortFunc(points, func(a, b promapi.VectorResultItem) int { return cmp.Compare(a.Metric["series"], b.Metric["series"]) })
		}
		sortVector(want.Data.Vector.Result)
		sortVector(got.Data.Vector.Result)
		require.Len(t, got.Data.Vector.Result, 2)
		for i, point := range want.Data.Vector.Result {
			require.Equal(t, point.Metric, got.Data.Vector.Result[i].Metric)
			wantValue, ok := point.Value.HistogramOrValue.GetStringFloat64()
			require.True(t, ok)
			gotValue, ok := got.Data.Vector.Result[i].Value.HistogramOrValue.GetStringFloat64()
			require.True(t, ok)
			require.InDelta(t, wantValue, gotValue, 1e-9)
		}
	})

	t.Run("NativeHistogramErrors", func(t *testing.T) {
		md := pmetric.NewMetrics()
		metric := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		metric.SetName("scarecrow_histogram")
		point := metric.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
		point.SetTimestamp(pcommon.Timestamp(base.UnixNano()))
		point.SetCount(1)
		point.SetSum(1)
		point.Positive().BucketCounts().Append(1)
		require.NoError(t, inserter.ConsumeMetrics(ctx, md))
		for _, client := range clients {
			for _, query := range []string{"scarecrow_histogram", "rate(scarecrow_histogram[5m])"} {
				_, err := client.GetQueryRange(ctx, promapi.GetQueryRangeParams{Query: query, Start: getPromTS(pcommon.Timestamp(base.UnixNano())), End: getPromTS(pcommon.Timestamp(base.Add(time.Minute).UnixNano())), Step: promapi.NewOptString("15s")})
				require.ErrorContains(t, err, "native histogram samples")
			}
		}
	})
}
