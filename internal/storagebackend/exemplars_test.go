package storagebackend_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

var (
	testTraceID = pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	testSpanID  = pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
)

// exemplarBackend ingests one gauge series carrying three exemplars, one per second from ts.
func exemplarBackend(t *testing.T, ts time.Time) (*storagebackend.Backend, context.Context) {
	t.Helper()

	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store)

	md := pmetric.NewMetrics()

	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "api")

	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("request_duration")

	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	dp.SetDoubleValue(1)
	dp.Attributes().PutStr("http.route", "/things")

	withTrace := dp.Exemplars().AppendEmpty()
	withTrace.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	withTrace.SetDoubleValue(0.25)
	withTrace.SetTraceID(testTraceID)
	withTrace.SetSpanID(testSpanID)
	withTrace.FilteredAttributes().PutStr("pod", "api-0")

	bare := dp.Exemplars().AppendEmpty()
	bare.SetTimestamp(pcommon.Timestamp(ts.Add(time.Second).UnixNano()))
	bare.SetIntValue(2)

	traceOnly := dp.Exemplars().AppendEmpty()
	traceOnly.SetTimestamp(pcommon.Timestamp(ts.Add(2 * time.Second).UnixNano()))
	traceOnly.SetDoubleValue(0.75)
	traceOnly.SetTraceID(testTraceID)

	require.NoError(t, b.ConsumeMetrics(ctx, md))

	return b, ctx
}

func selectExemplars(
	ctx context.Context, t *testing.T, b *storagebackend.Backend, startMs, endMs int64, matchers ...*labels.Matcher,
) []labels.Labels {
	t.Helper()

	q, err := b.ExemplarQuerier(ctx)
	require.NoError(t, err)

	res, err := q.Select(startMs, endMs, matchers)
	require.NoError(t, err)

	var out []labels.Labels
	for _, qr := range res {
		for _, e := range qr.Exemplars {
			require.True(t, e.HasTs)
			out = append(out, e.Labels)
		}
	}

	return out
}

// TestBackendExemplarsRoundtrip ingests exemplars through the OTLP sink and reads them back through
// the Prometheus exemplar seam, which is what /api/v1/query_exemplars is served from.
func TestBackendExemplarsRoundtrip(t *testing.T) {
	ts := time.Now().Truncate(time.Second)
	b, ctx := exemplarBackend(t, ts)

	q, err := b.ExemplarQuerier(ctx)
	require.NoError(t, err)

	res, err := q.Select(
		ts.Add(-time.Minute).UnixMilli(), ts.Add(time.Minute).UnixMilli(),
		[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "request_duration")},
	)
	require.NoError(t, err)
	require.Len(t, res, 1)

	got := res[0]
	require.Equal(t, "request_duration", got.SeriesLabels.Get(model.MetricNameLabel))
	require.Equal(t, "api", got.SeriesLabels.Get("service.name"))
	require.Equal(t, "/things", got.SeriesLabels.Get("http.route"))

	require.Len(t, got.Exemplars, 3)

	require.Equal(t, 0.25, got.Exemplars[0].Value)
	require.Equal(t, ts.UnixMilli(), got.Exemplars[0].Ts)
	require.Equal(t, "0102030405060708090a0b0c0d0e0f10", got.Exemplars[0].Labels.Get("trace_id"))
	require.Equal(t, "0102030405060708", got.Exemplars[0].Labels.Get("span_id"))
	require.Equal(t, "api-0", got.Exemplars[0].Labels.Get("pod"))

	// An exemplar the producer had no active span for carries no trace context at all, rather
	// than an all-zero id rendered as hex.
	require.Equal(t, float64(2), got.Exemplars[1].Value)
	require.Equal(t, labels.EmptyLabels(), got.Exemplars[1].Labels)

	require.Equal(t, 0.75, got.Exemplars[2].Value)
	require.Equal(t, "0102030405060708090a0b0c0d0e0f10", got.Exemplars[2].Labels.Get("trace_id"))
	require.Empty(t, got.Exemplars[2].Labels.Get("span_id"))
}

func TestBackendExemplarsSelect(t *testing.T) {
	ts := time.Now().Truncate(time.Second)
	b, ctx := exemplarBackend(t, ts)

	lo, hi := ts.Add(-time.Minute).UnixMilli(), ts.Add(time.Minute).UnixMilli()

	for _, tt := range []struct {
		name     string
		startMs  int64
		endMs    int64
		matchers []*labels.Matcher
		want     int
	}{
		{
			name:     "name",
			startMs:  lo,
			endMs:    hi,
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "request_duration")},
			want:     3,
		},
		{
			name:    "resource label",
			startMs: lo,
			endMs:   hi,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "request_duration"),
				labels.MustNewMatcher(labels.MatchEqual, "service.name", "api"),
			},
			want: 3,
		},
		{
			name:    "negated label",
			startMs: lo,
			endMs:   hi,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "request_duration"),
				labels.MustNewMatcher(labels.MatchNotEqual, "service.name", "api"),
			},
		},
		{
			name:     "other metric",
			startMs:  lo,
			endMs:    hi,
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "other")},
		},
		{
			name:     "unbounded window",
			startMs:  promapi.MinTime.UnixMilli(),
			endMs:    promapi.MaxTime.UnixMilli(),
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "request_duration")},
			want:     3,
		},
		{
			name:     "window excludes the first exemplar",
			startMs:  ts.Add(time.Second).UnixMilli(),
			endMs:    hi,
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "request_duration")},
			want:     2,
		},
		{
			name:     "window before the data",
			startMs:  ts.Add(-time.Hour).UnixMilli(),
			endMs:    ts.Add(-time.Minute).UnixMilli(),
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "request_duration")},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Len(t, selectExemplars(ctx, t, b, tt.startMs, tt.endMs, tt.matchers...), tt.want)
		})
	}
}

// multiExemplarRoutes are the series of [multiExemplarBackend], in an ingest order the engine
// returns unsorted — so a result that is not sorted by label set shows up as one.
var multiExemplarRoutes = []string{"/z", "/y", "/x", "/b", "/a"}

// multiExemplarBackend ingests one series of one metric per route, each carrying one exemplar.
func multiExemplarBackend(t *testing.T, ts time.Time) (*storagebackend.Backend, context.Context) {
	t.Helper()

	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store)

	md := pmetric.NewMetrics()

	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "api")

	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("request_duration")

	dps := m.SetEmptyGauge().DataPoints()

	for i, route := range multiExemplarRoutes {
		dp := dps.AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
		dp.SetDoubleValue(float64(i))
		dp.Attributes().PutStr("http.route", route)

		e := dp.Exemplars().AppendEmpty()
		e.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
		e.SetDoubleValue(float64(i))
		e.SetTraceID(testTraceID)
	}

	require.NoError(t, b.ConsumeMetrics(ctx, md))

	return b, ctx
}

// TestBackendExemplarsOverlappingMatcherSets pins that a series selected by more than one matcher
// set yields one result, not one per set. PromQL reaches Select with a set per selector, so a query
// naming the same metric twice (`a / a{x="y"}`) hits this.
func TestBackendExemplarsOverlappingMatcherSets(t *testing.T) {
	ts := time.Now().Truncate(time.Second)
	b, ctx := multiExemplarBackend(t, ts)

	q, err := b.ExemplarQuerier(ctx)
	require.NoError(t, err)

	res, err := q.Select(
		ts.Add(-time.Minute).UnixMilli(), ts.Add(time.Minute).UnixMilli(),
		[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "request_duration")},
		[]*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "request_duration"),
			labels.MustNewMatcher(labels.MatchEqual, "service.name", "api"),
		},
	)
	require.NoError(t, err)

	// Every series appears once, whole, and the results are ordered by label set.
	var routes []string

	for _, qr := range res {
		require.Len(t, qr.Exemplars, 1)
		routes = append(routes, qr.SeriesLabels.Get("http.route"))
	}

	require.Equal(t, []string{"/a", "/b", "/x", "/y", "/z"}, routes)
}

// TestBackendExemplarsEndBoundInclusive pins that an exemplar whose reported millisecond equals the
// requested end is returned: the fetch window is nanoseconds, but a client compares the Ts it gets
// back, which Prometheus includes at `e.Ts == end`.
func TestBackendExemplarsEndBoundInclusive(t *testing.T) {
	ts := time.Now().Truncate(time.Second)

	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store)

	md := pmetric.NewMetrics()

	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "api")

	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("request_duration")

	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	dp.SetDoubleValue(1)

	e := dp.Exemplars().AppendEmpty()
	e.SetTimestamp(pcommon.Timestamp(ts.Add(500 * time.Microsecond).UnixNano()))
	e.SetDoubleValue(2)

	require.NoError(t, b.ConsumeMetrics(ctx, md))

	matcher := labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "request_duration")

	got := selectExemplars(ctx, t, b, ts.Add(-time.Minute).UnixMilli(), ts.UnixMilli(), matcher)
	require.Len(t, got, 1)
}

func TestBackendExemplarsEmpty(t *testing.T) {
	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	q, err := storagebackend.New(store).ExemplarQuerier(ctx)
	require.NoError(t, err)

	res, err := q.Select(
		promapi.MinTime.UnixMilli(), promapi.MaxTime.UnixMilli(),
		[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "nothing")},
	)
	require.NoError(t, err)
	require.Empty(t, res)
}
