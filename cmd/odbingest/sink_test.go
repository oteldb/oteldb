package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/router"
	"github.com/oteldb/storage/signal"
)

func TestClusterSinkRecordsRetentionRejects(t *testing.T) {
	t.Parallel()
	for _, sig := range []signal.Signal{signal.Metric, signal.Log, signal.Trace, signal.Profile} {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(t.Context())) })
			sink, err := newClusterSink(nil, nil, provider)
			require.NoError(t, err)
			require.NoError(t, sink.record(t.Context(), sig, router.Written{
				Accepted: 3,
				Rejected: cluster.Reject{OOO: 1, Cardinality: 2, InFlight: 3, Retention: 4},
			}, nil))

			var metrics metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &metrics))
			var accepted int64
			rejected := map[string]int64{}
			for _, scope := range metrics.ScopeMetrics {
				for _, m := range scope.Metrics {
					sum, ok := m.Data.(metricdata.Sum[int64])
					require.True(t, ok)
					for _, point := range sum.DataPoints {
						gotSig, ok := point.Attributes.Value(signalKey)
						require.True(t, ok)
						assert.Equal(t, sig.String(), gotSig.AsString())
						switch m.Name {
						case "odbingest.cluster.accepted_points":
							accepted += point.Value
						case "odbingest.cluster.rejected_points":
							reason, ok := point.Attributes.Value(reasonKey)
							require.True(t, ok)
							rejected[reason.AsString()] += point.Value
						}
					}
				}
			}
			assert.EqualValues(t, 3, accepted)
			assert.Equal(t, map[string]int64{"out_of_order": 1, "max_series": 2, "max_in_flight": 3, "retention": 4}, rejected)
		})
	}
}
