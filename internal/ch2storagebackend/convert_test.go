package ch2storagebackend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage/otlp/pdataconv"
	siglog "github.com/oteldb/storage/signal/log"
	sigmetric "github.com/oteldb/storage/signal/metric"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/metricstorage"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

func attrs(kvs map[string]any) otelstorage.Attrs {
	m := pcommon.NewMap()
	for k, v := range kvs {
		switch v := v.(type) {
		case string:
			m.PutStr(k, v)
		case int:
			m.PutInt(k, int64(v))
		case float64:
			m.PutDouble(k, v)
		case bool:
			m.PutBool(k, v)
		default:
			panic("unsupported attribute type")
		}
	}
	return otelstorage.Attrs(m)
}

// sampleRecords spans several streams, attribute types, and an empty-attribute record, so the
// equivalence check exercises the grouping and the value projection, not just the happy path.
func sampleRecords() []logstorage.Record {
	res := attrs(map[string]any{"service.name": "api", "host": "a"})
	other := attrs(map[string]any{"service.name": "worker"})
	scope := attrs(map[string]any{"lib": "zap"})

	return []logstorage.Record{
		{
			Timestamp: 100, ObservedTimestamp: 101,
			SeverityText: "INFO", SeverityNumber: plog.SeverityNumberInfo,
			Body:          "hello",
			TraceID:       otelstorage.TraceID{1, 2, 3},
			SpanID:        otelstorage.SpanID{4, 5},
			Flags:         plog.LogRecordFlags(1),
			Attrs:         attrs(map[string]any{"code": 200, "ok": true, "ratio": 0.5}),
			ResourceAttrs: res, ScopeName: "zap", ScopeVersion: "1.0", ScopeAttrs: scope,
		},
		{
			// Same stream as above: must land under the same resource/scope.
			Timestamp: 200, Body: "second", Attrs: attrs(map[string]any{"code": 500}),
			ResourceAttrs: res, ScopeName: "zap", ScopeVersion: "1.0", ScopeAttrs: scope,
		},
		{
			// Different resource, no ids, empty attribute set.
			Timestamp: 300, Body: "third", Attrs: attrs(nil),
			ResourceAttrs: other, ScopeName: "zap", ScopeVersion: "1.0", ScopeAttrs: scope,
		},
		{
			// Same resource as the first, different scope version: a distinct stream.
			Timestamp: 400, Body: "fourth", Attrs: attrs(nil),
			ResourceAttrs: res, ScopeName: "zap", ScopeVersion: "2.0", ScopeAttrs: scope,
		},
	}
}

func sampleSpans() []tracestorage.Span {
	res := attrs(map[string]any{"service.name": "api"})
	other := attrs(map[string]any{"service.name": "db"})
	scope := attrs(map[string]any{"lib": "otel"})

	return []tracestorage.Span{
		{
			TraceID: otelstorage.TraceID{1}, SpanID: otelstorage.SpanID{2},
			ParentSpanID: otelstorage.SpanID{3},
			TraceState:   "vendor=1",
			Name:         "GET /", Kind: 2,
			Start: 1000, End: 2000,
			Attrs:      attrs(map[string]any{"http.status": 200}),
			StatusCode: 1, StatusMessage: "ok",
			ResourceAttrs: res, ScopeName: "otel", ScopeVersion: "1.0", ScopeAttrs: scope,
			Events: []tracestorage.Event{
				{Timestamp: 1500, Name: "cache.miss", Attrs: attrs(map[string]any{"key": "k"})},
				{Timestamp: 1600, Name: "retry", Attrs: attrs(nil)},
			},
			Links: []tracestorage.Link{
				{TraceID: otelstorage.TraceID{9}, SpanID: otelstorage.SpanID{8}, TraceState: "x=1", Attrs: attrs(nil)},
			},
		},
		{
			// No parent, no events/links, empty attrs — the minimal span.
			TraceID: otelstorage.TraceID{4}, SpanID: otelstorage.SpanID{5},
			Name: "query", Start: 1100, End: 1200, Attrs: attrs(nil),
			ResourceAttrs: other, ScopeName: "otel", ScopeVersion: "1.0", ScopeAttrs: scope,
		},
	}
}

func samplePoints() []metricstorage.NumberPoint {
	res := attrs(map[string]any{"service.name": "api"})
	other := attrs(map[string]any{"service.name": "worker"})
	scope := attrs(map[string]any{})
	series := attrs(map[string]any{"route": "/a"})

	return []metricstorage.NumberPoint{
		{Name: "http_requests", Unit: "1", Resource: res, Scope: scope, Attrs: series, Timestamp: 100, Value: 1},
		// Same series, later point: must accumulate under the same Metric.
		{Name: "http_requests", Unit: "1", Resource: res, Scope: scope, Attrs: series, Timestamp: 200, Value: 2},
		// Same stream, different name: a second Metric under the same scope.
		{Name: "http_errors", Unit: "1", Resource: res, Scope: scope, Attrs: series, Timestamp: 100, Value: 3},
		// Different resource: a second stream.
		{Name: "http_requests", Unit: "1", Resource: other, Scope: scope, Attrs: series, Timestamp: 100, Value: 4},
		// No datapoint attributes.
		{Name: "uptime", Unit: "s", Resource: res, Scope: scope, Attrs: attrs(nil), Timestamp: 100, Value: 5},
	}
}

// The direct converters exist only as a faster route to the same batch the OTLP path produces.
// These tests pin that equivalence, so a drift in either path fails here rather than silently
// changing what a migration writes.
//
// One field is normalized before comparing. chstorage stores no schema URL for any signal, so both
// paths mean "absent" — but the pdata route spells it []byte("") (the artifact of converting an
// empty Go string) while the direct route leaves it nil. Nothing downstream distinguishes the two,
// and nil avoids the allocation, so the difference is normalized away rather than reproduced.
func nilIfEmpty(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

func normalizeLogs(l *siglog.Logs) {
	for i := range l.Resources {
		r := &l.Resources[i]
		r.Resource.SchemaURL = nilIfEmpty(r.Resource.SchemaURL)
		for j := range r.Scopes {
			r.Scopes[j].Scope.SchemaURL = nilIfEmpty(r.Scopes[j].Scope.SchemaURL)
		}
	}
}

func normalizeTraces(t *sigtrace.Traces) {
	for i := range t.Resources {
		r := &t.Resources[i]
		r.Resource.SchemaURL = nilIfEmpty(r.Resource.SchemaURL)
		for j := range r.Scopes {
			r.Scopes[j].Scope.SchemaURL = nilIfEmpty(r.Scopes[j].Scope.SchemaURL)
		}
	}
}

// normalizeMetrics also nils the scope name/version: chstorage stores neither for metrics (only a
// scope attribute map), so the same empty-string-vs-nil artifact applies to them.
func normalizeMetrics(m *sigmetric.Metrics) {
	for i := range m.Resources {
		r := &m.Resources[i]
		r.Resource.SchemaURL = nilIfEmpty(r.Resource.SchemaURL)
		for j := range r.Scopes {
			sc := &r.Scopes[j].Scope
			sc.SchemaURL = nilIfEmpty(sc.SchemaURL)
			sc.Name = nilIfEmpty(sc.Name)
			sc.Version = nilIfEmpty(sc.Version)
		}
	}
}

func TestConvertLogsMatchesPDATA(t *testing.T) {
	records := sampleRecords()

	var want siglog.Logs
	pdataconv.AppendLogs(&want, logstorage.RecordsToLogs(records))

	got := ConvertLogs(records, newAttrConv())

	normalizeLogs(&want)
	normalizeLogs(&got)
	require.Equal(t, want, got)
}

func TestConvertTracesMatchesPDATA(t *testing.T) {
	spans := sampleSpans()

	var want sigtrace.Traces
	pdataconv.AppendTraces(&want, tracestorage.SpansToTraces(spans))

	got := ConvertTraces(spans, newAttrConv())

	normalizeTraces(&want)
	normalizeTraces(&got)
	require.Equal(t, want, got)
}

func TestConvertNumberPointsMatchesPDATA(t *testing.T) {
	points := samplePoints()

	var want sigmetric.Metrics
	pdataconv.AppendMetrics(&want, metricstorage.NumberPointsToMetrics(points))

	got := ConvertNumberPoints(points, newAttrConv())

	normalizeMetrics(&want)
	normalizeMetrics(&got)
	require.Equal(t, want, got)
}

func TestConvertEmptyBatches(t *testing.T) {
	c := newAttrConv()
	assert.Empty(t, ConvertLogs(nil, c).Resources)
	assert.Empty(t, ConvertTraces(nil, c).Resources)
	assert.Empty(t, ConvertNumberPoints(nil, c).Resources)
}

func TestConvertGrouping(t *testing.T) {
	t.Run("logs group by resource and scope", func(t *testing.T) {
		got := ConvertLogs(sampleRecords(), newAttrConv())

		// Three distinct streams: (api, zap 1.0), (worker, zap 1.0), (api, zap 2.0).
		require.Len(t, got.Resources, 3)
		assert.Len(t, got.Resources[0].Scopes[0].Records, 2, "same stream accumulates")
		assert.Len(t, got.Resources[1].Scopes[0].Records, 1)
		assert.Len(t, got.Resources[2].Scopes[0].Records, 1)
	})

	t.Run("points accumulate under one metric per name", func(t *testing.T) {
		got := ConvertNumberPoints(samplePoints(), newAttrConv())

		require.Len(t, got.Resources, 2)
		metrics := got.Resources[0].Scopes[0].Metrics
		require.Len(t, metrics, 3, "http_requests, http_errors, uptime")

		assert.Equal(t, []byte("http_requests"), metrics[0].Name)
		assert.Len(t, metrics[0].Points, 2, "two points of the same series")
		assert.Equal(t, sigmetric.KindGauge, metrics[0].Kind)
	})
}

// A grouping map that cached pointers into the batch's backing arrays would be writing into
// abandoned arrays once a later append reallocated them. This drives enough distinct streams to
// force several reallocations and checks nothing was lost.
func TestConvertSurvivesBackingArrayGrowth(t *testing.T) {
	const streams = 64

	var records []logstorage.Record
	for i := range streams {
		res := attrs(map[string]any{"service.name": "svc", "shard": i})
		// Two records per stream, interleaved across streams so the map is hit after growth.
		records = append(records, logstorage.Record{Timestamp: int64ToTS(i), Body: "a", Attrs: attrs(nil), ResourceAttrs: res})
	}
	for i := range streams {
		res := attrs(map[string]any{"service.name": "svc", "shard": i})
		records = append(records, logstorage.Record{Timestamp: int64ToTS(i), Body: "b", Attrs: attrs(nil), ResourceAttrs: res})
	}

	got := ConvertLogs(records, newAttrConv())
	require.Len(t, got.Resources, streams)

	total := 0
	for _, r := range got.Resources {
		require.Len(t, r.Scopes, 1)
		total += len(r.Scopes[0].Records)
		assert.Len(t, r.Scopes[0].Records, 2, "both records landed in the same stream")
	}
	assert.Equal(t, len(records), total, "no record lost to a reallocated slice")
}

func int64ToTS(i int) otelstorage.Timestamp {
	return otelstorage.Timestamp(uint64(i + 1))
}

func TestAttrConvMemoizes(t *testing.T) {
	c := newAttrConv()
	a := attrs(map[string]any{"service.name": "api"})

	first := c.convert(a)
	second := c.convert(a)

	require.Equal(t, first, second)
	assert.Len(t, c.cache, 1, "an identical attribute set is converted once")

	// A distinct set gets its own entry.
	c.convert(attrs(map[string]any{"service.name": "worker"}))
	assert.Len(t, c.cache, 2)

	// An empty or unset map converts to nil without occupying the cache.
	assert.Nil(t, c.convert(attrs(map[string]any{})))
	assert.Nil(t, c.convert(otelstorage.Attrs{}))
	assert.Len(t, c.cache, 2)
}

func TestConvertValueTypes(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("str", "s")
	m.PutInt("int", 7)
	m.PutDouble("double", 1.5)
	m.PutBool("bool", true)
	m.PutEmptyBytes("bytes").Append(1, 2, 3)
	m.PutEmpty("empty")
	sl := m.PutEmptySlice("slice")
	sl.AppendEmpty().SetStr("a")
	sl.AppendEmpty().SetInt(2)
	nested := m.PutEmptyMap("map")
	nested.PutStr("k", "v")

	// pdataconv is the reference projection; the local copy must agree on every value type.
	var want siglog.Logs
	ld := plog.NewLogs()
	rec := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	m.CopyTo(rec.Attributes())
	pdataconv.AppendLogs(&want, ld)

	assert.Equal(t, want.Resources[0].Scopes[0].Records[0].Attributes, convertMap(m))
}
