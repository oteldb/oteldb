package storagebackend_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/oteldb/internal/otlpdirect"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// TestMetricIngestPathsAgreeOnExemplars pins that oteldb's two OTLP metric ingest paths — the
// direct wire decoder odbingest serves and the collector consumer over pdataconv — store the same
// exemplars, read back through the Prometheus exemplar seam.
//
// They disagreed before: the direct decoder skipped the number point's exemplars field entirely, so
// exemplars vanished on the production ingest path while the pdata path kept them.
func TestMetricIngestPathsAgreeOnExemplars(t *testing.T) {
	ts := time.Now().Truncate(time.Second)
	md := metricsWithExemplars(ts)

	viaCollector := exemplarsViaCollector(t, md)
	viaDirect := exemplarsViaOTLPDirect(t, md)

	require.NotEmpty(t, viaCollector)
	require.Equal(t, viaCollector, viaDirect, "both ingest paths must store the same exemplars")
}

// metricsWithExemplars builds a gauge and a sum whose points carry exemplars with and without a
// trace context, plus a value-less one neither path can represent.
func metricsWithExemplars(ts time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()

	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "api")

	sm := rm.ScopeMetrics().AppendEmpty()

	g := sm.Metrics().AppendEmpty()
	g.SetName("request_duration")

	gp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	gp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	gp.SetDoubleValue(1)
	gp.Attributes().PutStr("http.route", "/things")

	full := gp.Exemplars().AppendEmpty()
	full.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	full.SetDoubleValue(0.25)
	full.SetTraceID(testTraceID)
	full.SetSpanID(testSpanID)
	full.FilteredAttributes().PutStr("pod", "api-0")

	gp.Exemplars().AppendEmpty().SetTimestamp(pcommon.Timestamp(ts.UnixNano())) // value-less

	s := sm.Metrics().AppendEmpty()
	s.SetName("request_count")

	sum := s.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)

	sp := sum.DataPoints().AppendEmpty()
	sp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	sp.SetIntValue(3)

	bare := sp.Exemplars().AppendEmpty()
	bare.SetTimestamp(pcommon.Timestamp(ts.Add(time.Second).UnixNano()))
	bare.SetIntValue(2)

	return md
}

func exemplarsViaCollector(t *testing.T, md pmetric.Metrics) []exemplar.QueryResult {
	t.Helper()

	b, ctx := backendWithMeter(t, nil)
	require.NoError(t, b.ConsumeMetrics(ctx, md))

	return storedExemplars(ctx, t, b)
}

func exemplarsViaOTLPDirect(t *testing.T, md pmetric.Metrics) []exemplar.QueryResult {
	t.Helper()

	b, ctx, _ := postOTLPMetrics(t, md)

	return storedExemplars(ctx, t, b)
}

// postOTLPMetrics posts md to the [otlpdirect] handler odbingest serves, returning the backend it
// wrote to and the stats the handler reported.
func postOTLPMetrics(t *testing.T, md pmetric.Metrics) (*storagebackend.Backend, context.Context, otlpdirect.Stats) {
	t.Helper()

	b, ctx := backendWithMeter(t, nil)

	var stats []otlpdirect.Stats

	mux := http.NewServeMux()
	otlpdirect.NewHandler(backendSink{b}, otlpdirect.HandlerConfig{
		Observer: func(s otlpdirect.Stats) { stats = append(stats, s) },
	}).Register(mux)

	raw, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, otlpdirect.MetricsPath, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/x-protobuf")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body)
	require.Len(t, stats, 1)

	return b, ctx, stats[0]
}

// undroppableExemplars builds a batch whose exemplars cannot be stored: two on a histogram point,
// which classic decomposition leaves no series to hang off, and one on a value-less number point,
// which is dropped and takes its exemplar with it.
func undroppableExemplars(ts time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()

	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "api")

	sm := rm.ScopeMetrics().AppendEmpty()

	h := sm.Metrics().AppendEmpty()
	h.SetName("request_duration")

	hist := h.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	hp := hist.DataPoints().AppendEmpty()
	hp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	hp.SetCount(2)
	hp.ExplicitBounds().FromRaw([]float64{1})
	hp.BucketCounts().FromRaw([]uint64{1, 1})

	for range 2 {
		e := hp.Exemplars().AppendEmpty()
		e.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
		e.SetDoubleValue(1)
		e.SetTraceID(testTraceID)
	}

	g := sm.Metrics().AppendEmpty()
	g.SetName("request_count")

	gp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	gp.SetTimestamp(pcommon.Timestamp(ts.UnixNano())) // no value

	ge := gp.Exemplars().AppendEmpty()
	ge.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	ge.SetDoubleValue(1)

	return md
}

// TestConsumeMetricsCountsDropped pins that the collector sink reports what it could not store.
// Its signature returns only an error, so this counter is the only place those become visible.
func TestConsumeMetricsCountsDropped(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, ctx := backendWithMeter(t, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	require.NoError(t, b.ConsumeMetrics(ctx, undroppableExemplars(time.Now().Truncate(time.Second))))

	require.Equal(t, map[string]int64{"exemplar": 3, "no_value": 1}, droppedByReason(ctx, t, reader))
}

// TestOTLPDirectReportsDroppedExemplars pins that the odbingest path reports the same drops through
// its ingest stats, which is where cmd/odbingest's counters come from.
func TestOTLPDirectReportsDroppedExemplars(t *testing.T) {
	_, _, stats := postOTLPMetrics(t, undroppableExemplars(time.Now().Truncate(time.Second)))

	require.Equal(t, 3, stats.DroppedExemplars)
	// Partial success stays a point count: a lost exemplar is not a refused data point.
	require.Equal(t, 1, stats.Rejected)
}

// droppedByReason sums oteldb.storage.dropped_records for the metrics signal, keyed by reason.
func droppedByReason(ctx context.Context, t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	out := map[string]int64{}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "oteldb.storage.dropped_records" {
				continue
			}

			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)

			for _, dp := range sum.DataPoints {
				sig, _ := dp.Attributes.Value(attribute.Key("signal"))
				if sig.AsString() != "metric" {
					continue
				}

				reason, _ := dp.Attributes.Value(attribute.Key("reason"))
				out[reason.AsString()] += dp.Value
			}
		}
	}

	return out
}

func storedExemplars(ctx context.Context, t *testing.T, b *storagebackend.Backend) []exemplar.QueryResult {
	t.Helper()

	q, err := b.ExemplarQuerier(ctx)
	require.NoError(t, err)

	res, err := q.Select(
		promapi.MinTime.UnixMilli(), promapi.MaxTime.UnixMilli(),
		[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "service.name", "api")},
	)
	require.NoError(t, err)

	return res
}
