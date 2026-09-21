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

	b, ctx := backendWithMeter(t, nil)

	mux := http.NewServeMux()
	otlpdirect.NewHandler(backendSink{b}, otlpdirect.HandlerConfig{}).Register(mux)

	raw, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, otlpdirect.MetricsPath, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/x-protobuf")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body)

	return storedExemplars(ctx, t, b)
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
