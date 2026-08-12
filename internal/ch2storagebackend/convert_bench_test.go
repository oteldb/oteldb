package ch2storagebackend

import (
	"strconv"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/storage/otlp/pdataconv"
	siglog "github.com/oteldb/storage/signal/log"
	sigmetric "github.com/oteldb/storage/signal/metric"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/metricstorage"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// benchSeriesAttrs builds n distinct series attribute sets, mimicking how chstorage resolves a
// row's identity: the same map instance is handed back for every row of a series, which is what the
// direct path's memoization exploits.
func benchSeriesAttrs(n int) []otelstorage.Attrs {
	out := make([]otelstorage.Attrs, n)
	for i := range out {
		m := pcommon.NewMap()
		m.PutStr("route", "/route/"+strconv.Itoa(i))
		m.PutStr("method", "GET")
		out[i] = otelstorage.Attrs(m)
	}
	return out
}

func benchResource() otelstorage.Attrs {
	m := pcommon.NewMap()
	m.PutStr("service.name", "bench")
	m.PutStr("host.name", "node-1")
	return otelstorage.Attrs(m)
}

// benchPoints lays out rows the way a metrics scan delivers them: pointsPerSeries samples for each
// of seriesCount series, all sharing one resource and scope.
func benchPoints(seriesCount, pointsPerSeries int) []metricstorage.NumberPoint {
	var (
		res    = benchResource()
		scope  = otelstorage.Attrs(pcommon.NewMap())
		series = benchSeriesAttrs(seriesCount)
		out    = make([]metricstorage.NumberPoint, 0, seriesCount*pointsPerSeries)
	)
	for s := range seriesCount {
		for p := range pointsPerSeries {
			out = append(out, metricstorage.NumberPoint{
				Name: "http.requests", Unit: "1",
				Resource: res, Scope: scope, Attrs: series[s],
				Timestamp: otelstorage.Timestamp(uint64(1_000_000_000 + p*15_000)),
				Value:     float64(p),
			})
		}
	}
	return out
}

func benchRecords(n int) []logstorage.Record {
	var (
		res   = benchResource()
		scope = otelstorage.Attrs(pcommon.NewMap())
		attrs = benchSeriesAttrs(16)
		out   = make([]logstorage.Record, 0, n)
	)
	for i := range n {
		out = append(out, logstorage.Record{
			Timestamp: otelstorage.Timestamp(uint64(i + 1)),
			Body:      "GET /api/v1/resource 200 in 13ms",
			Attrs:     attrs[i%len(attrs)],
			TraceID:   otelstorage.TraceID{byte(i), 2, 3},
			SpanID:    otelstorage.SpanID{byte(i), 5},

			ResourceAttrs: res, ScopeName: "zap", ScopeVersion: "1.0", ScopeAttrs: scope,
		})
	}
	return out
}

func benchSpans(n int) []tracestorage.Span {
	var (
		res   = benchResource()
		scope = otelstorage.Attrs(pcommon.NewMap())
		attrs = benchSeriesAttrs(16)
		out   = make([]tracestorage.Span, 0, n)
	)
	for i := range n {
		out = append(out, tracestorage.Span{
			TraceID: otelstorage.TraceID{byte(i), 1}, SpanID: otelstorage.SpanID{byte(i), 2},
			Name: "GET /api", Kind: 2,
			Start: otelstorage.Timestamp(uint64(i * 1000)),
			End:   otelstorage.Timestamp(uint64(i*1000 + 500)),
			Attrs: attrs[i%len(attrs)], StatusCode: 1, StatusMessage: "ok",

			ResourceAttrs: res, ScopeName: "otel", ScopeVersion: "1.0", ScopeAttrs: scope,
		})
	}
	return out
}

func reportRows(b *testing.B, rows int) {
	b.Helper()

	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(rows)*float64(b.N)/secs/1e6, "Mrows/s")
	}
}

// The Direct/PDATA pairs below measure exactly what item 2 bought: the same batch, built with and
// without the intermediate OTLP tree.

func BenchmarkConvertNumberPoints(b *testing.B) {
	shapes := []struct {
		name            string
		series, perSeri int
	}{
		{"1000series_5points", 1000, 5},
		{"100series_50points", 100, 50},
		{"5000series_1point", 5000, 1},
	}

	for _, sh := range shapes {
		points := benchPoints(sh.series, sh.perSeri)
		rows := len(points)

		b.Run(sh.name+"/Direct", func(b *testing.B) {
			c := newAttrConv()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				sink(ConvertNumberPoints(points, c))
			}
			reportRows(b, rows)
		})

		b.Run(sh.name+"/PDATA", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var dst sigmetric.Metrics
				pdataconv.AppendMetrics(&dst, metricstorage.NumberPointsToMetrics(points))
				sink(dst)
			}
			reportRows(b, rows)
		})
	}
}

func BenchmarkConvertLogs(b *testing.B) {
	records := benchRecords(5000)

	b.Run("Direct", func(b *testing.B) {
		c := newAttrConv()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			sink(ConvertLogs(records, c))
		}
		reportRows(b, len(records))
	})

	b.Run("PDATA", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var dst siglog.Logs
			pdataconv.AppendLogs(&dst, logstorage.RecordsToLogs(records))
			sink(dst)
		}
		reportRows(b, len(records))
	})
}

func BenchmarkConvertTraces(b *testing.B) {
	spans := benchSpans(5000)

	b.Run("Direct", func(b *testing.B) {
		c := newAttrConv()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			sink(ConvertTraces(spans, c))
		}
		reportRows(b, len(spans))
	})

	b.Run("PDATA", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var dst sigtrace.Traces
			pdataconv.AppendTraces(&dst, tracestorage.SpansToTraces(spans))
			sink(dst)
		}
		reportRows(b, len(spans))
	})
}

//go:noinline
func sink(any) {}
