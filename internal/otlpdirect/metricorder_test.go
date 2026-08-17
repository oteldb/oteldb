package otlpdirect_test

import (
	"testing"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// otlpCumulative is AGGREGATION_TEMPORALITY_CUMULATIVE on the wire.
const otlpCumulative = 2

// encodeScopeMetrics builds one ExportMetricsServiceRequest carrying a histogram metric, writing
// each message's fields ascending or descending. A metric's name and unit are siblings of the
// oneof arm that holds its points, so the ordering hazard reaches the metric identity itself here,
// not just a schema URL.
func encodeScopeMetrics(ascending bool) []byte {
	var m easyproto.Marshaler

	req := m.MessageMarshaler()
	rm := req.AppendMessage(1) // resource_metrics

	writeResource := func() {
		res := rm.AppendMessage(1)
		kv := res.AppendMessage(1)
		kv.AppendString(1, "service.name")
		kv.AppendMessage(2).AppendString(1, "api")
	}

	writeScopes := func() {
		sm := rm.AppendMessage(2) // scope_metrics

		writeScope := func() {
			sc := sm.AppendMessage(1)
			sc.AppendString(1, "scope-name")
			sc.AppendString(2, "1.0.0")
		}

		writeMetrics := func() {
			mt := sm.AppendMessage(2) // metrics

			writeIdentity := func() {
				mt.AppendString(1, "http.server.duration") // name
				mt.AppendString(3, "ms")                   // unit
			}

			writeHistogram := func() {
				h := mt.AppendMessage(9) // histogram

				dp := h.AppendMessage(1) // data_points
				dp.AppendFixed64(2, 1_700_000_000_000_000_000)
				dp.AppendFixed64(3, 1_700_000_000_000_000_001)
				dp.AppendFixed64(4, 6) // count
				dp.AppendDouble(5, 1.5)
				dp.AppendFixed64s(6, []uint64{1, 2, 3}) // bucket_counts
				dp.AppendDoubles(7, []float64{1, 2})    // explicit_bounds

				kv := dp.AppendMessage(9) // attributes
				kv.AppendString(1, "http.route")
				kv.AppendMessage(2).AppendString(1, "/things")

				h.AppendInt64(2, otlpCumulative)
			}

			if ascending {
				writeIdentity()
				writeHistogram()
			} else {
				writeHistogram()
				writeIdentity()
			}
		}

		writeSchema := func() { sm.AppendString(3, "https://schema.example/scope") }

		if ascending {
			writeScope()
			writeMetrics()
			writeSchema()
		} else {
			writeSchema()
			writeMetrics()
			writeScope()
		}
	}

	writeSchema := func() { rm.AppendString(3, "https://schema.example/resource") }

	if ascending {
		writeResource()
		writeScopes()
		writeSchema()
	} else {
		writeSchema()
		writeScopes()
		writeResource()
	}

	return m.Marshal(nil)
}

// TestConvertMetricsIsFieldOrderIndependent pins that the two wire orderings decode identically.
// A metric decoded before its name is read would produce correctly-shaped series under an empty
// name — data that looks fine until someone queries for it.
func TestConvertMetricsIsFieldOrderIndependent(t *testing.T) {
	t.Parallel()

	var asc, desc otlpdirect.MetricsConverter

	up, _, err := asc.Convert(encodeScopeMetrics(true))
	require.NoError(t, err)

	down, _, err := desc.Convert(encodeScopeMetrics(false))
	require.NoError(t, err)

	require.Equal(t, canonicalMetrics(down), canonicalMetrics(up))

	sm := up.Resources[0].Scopes[0]
	assert.Equal(t, "https://schema.example/scope", string(sm.Scope.SchemaURL))
	assert.Equal(t, "scope-name", string(sm.Scope.Name))
	assert.Equal(t, "https://schema.example/resource", string(up.Resources[0].Resource.SchemaURL))

	// The decomposed series must carry the metric's own name and unit, which arrive on the far
	// side of the histogram body in the descending encoding.
	require.Len(t, sm.Metrics, 1+1+3) // _count, _sum, three buckets

	for _, mt := range sm.Metrics {
		assert.Equal(t, "ms", string(mt.Unit))
		assert.Contains(t, string(mt.Name), "http.server.duration")
		assert.Equal(t, metric.TemporalityCumulative, mt.Temporality)
	}

	assert.Equal(t, "http.server.duration_count", string(sm.Metrics[0].Name))
	assert.Equal(t, "http.server.duration_sum", string(sm.Metrics[1].Name))
	assert.Equal(t, "http.server.duration_bucket", string(sm.Metrics[2].Name))
}

// canonicalMetrics is [canonical] for the metrics model. See canonical_test.go.
func canonicalMetrics(m *metric.Metrics) *metric.Metrics {
	for i := range m.Resources {
		rm := &m.Resources[i]
		rm.Resource.SchemaURL = canonBytes(rm.Resource.SchemaURL)
		rm.Resource.Attributes = canonAttrs(rm.Resource.Attributes)

		for j := range rm.Scopes {
			sm := &rm.Scopes[j]
			sm.Scope.Name = canonBytes(sm.Scope.Name)
			sm.Scope.Version = canonBytes(sm.Scope.Version)
			sm.Scope.SchemaURL = canonBytes(sm.Scope.SchemaURL)
			sm.Scope.Attributes = canonAttrs(sm.Scope.Attributes)

			for k := range sm.Metrics {
				mt := &sm.Metrics[k]
				mt.Name = canonBytes(mt.Name)
				mt.Unit = canonBytes(mt.Unit)

				for p := range mt.Points {
					mt.Points[p].Attributes = canonAttrs(mt.Points[p].Attributes)
				}

				if len(mt.Points) == 0 {
					mt.Points = nil
				}
			}

			if len(sm.Metrics) == 0 {
				sm.Metrics = nil
			}
		}
	}

	return m
}
