package storagebackend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
)

func TestProjectedMetricsRouting(t *testing.T) {
	for _, tt := range []struct {
		name    string
		prepare func(pmetric.Metric)
		handled bool
	}{
		{name: "gauge", prepare: func(pmetric.Metric) {}, handled: true},
		{name: "sum", prepare: func(m pmetric.Metric) { m.SetEmptySum().DataPoints().AppendEmpty().SetDoubleValue(1) }, handled: true},
		{name: "histogram", prepare: func(m pmetric.Metric) { m.SetEmptyHistogram().DataPoints().AppendEmpty().SetCount(1) }},
		{name: "exemplar", prepare: func(m pmetric.Metric) {
			m.Gauge().DataPoints().At(0).Exemplars().AppendEmpty().SetDoubleValue(1)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, err := storage.InMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
			backend := New(store)
			md := pmetric.NewMetrics()
			m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			m.SetName("projection_route")
			m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1)
			tt.prepare(m)
			handled, err := backend.consumeProjectedMetrics(t.Context(), md)
			require.NoError(t, err)
			require.Equal(t, tt.handled, handled)
			series, err := store.MetricSeries(t.Context(), "default", nil, 0, 0)
			require.NoError(t, err)
			if handled {
				require.Len(t, series, 1)
			} else {
				require.Empty(t, series, "fallback must not partially write the request")
				require.NoError(t, backend.ConsumeMetrics(t.Context(), md))
				series, err = store.MetricSeries(t.Context(), "default", nil, 0, 0)
				require.NoError(t, err)
				require.NotEmpty(t, series)
			}
		})
	}
}
