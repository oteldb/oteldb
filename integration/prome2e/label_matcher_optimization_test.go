package prome2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestStorageLabelMatcherEndpoints(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	db := storagebackend.New(store)
	md := pmetric.NewMetrics()
	metric := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("endpoint_metric")
	points := metric.SetEmptyGauge().DataPoints()
	for _, id := range []string{"foo", "bar", "subword", "number", "missing"} {
		point := points.AppendEmpty()
		point.SetTimestamp(pcommon.Timestamp(time.Unix(1_700_000_000, 0).UnixNano()))
		point.SetDoubleValue(1)
		point.Attributes().PutStr("id", id)
		switch id {
		case "foo", "bar":
			point.Attributes().PutStr("route", id)
		case "subword":
			point.Attributes().PutStr("route", "xfooy")
		case "number":
			point.Attributes().PutInt("route", 7)
		}
	}
	require.NoError(t, db.ConsumeMetrics(ctx, md))
	_, client := setupDB(t, integration.TraceProvider(t), db)
	for _, phase := range []string{"head", "parts"} {
		if phase == "parts" {
			require.NoError(t, store.Admin().Flush(ctx, "default", signal.Metric))
		}
		t.Run(phase, func(t *testing.T) {
			for _, tt := range []struct {
				selector string
				ids      []string
			}{
				{`endpoint_metric{route=~"foo|bar"}`, []string{"foo", "bar"}},
				{`endpoint_metric{route=~"foo|"}`, []string{"foo", "missing"}},
				{`endpoint_metric{route=~"7"}`, []string{"number"}},
				{`endpoint_metric{route!~"foo|bar"}`, []string{"subword", "number", "missing"}},
			} {
				t.Run(tt.selector, func(t *testing.T) {
					start, end := promapi.NewOptPrometheusTimestamp("1699999999"), promapi.NewOptPrometheusTimestamp("1700000001")
					series, err := client.GetSeries(ctx, promapi.GetSeriesParams{Match: []string{tt.selector}, Start: start, End: end})
					require.NoError(t, err)
					var ids []string
					for _, labels := range series.Data {
						ids = append(ids, labels["id"])
					}
					require.ElementsMatch(t, tt.ids, ids)
					values, err := client.GetLabelValues(ctx, promapi.GetLabelValuesParams{Label: "id", Match: []string{tt.selector}, Start: start, End: end})
					require.NoError(t, err)
					require.ElementsMatch(t, tt.ids, values.Data)
				})
			}
		})
	}
}
