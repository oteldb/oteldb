package odblogparser

import (
	"context"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap/zaptest"

	"github.com/oteldb/oteldb/internal/logparser"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/odbagent/internal/stanzatest"
)

func TestConfigBuild(t *testing.T) {
	t.Run("registered", func(t *testing.T) {
		builder, ok := operator.Lookup(Type)
		require.True(t, ok)
		require.IsType(t, &Config{}, builder())
	})

	t.Run("unknown format", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Format = "missing"

		_, err := cfg.Build(testTelemetrySettings(t))
		require.EqualError(t, err, `unknown log parser format "missing"`)
	})

	t.Run("explicit format skips detect formats", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Format = "generic-json"
		cfg.DetectFormats = []string{"missing"}

		_, err := cfg.Build(testTelemetrySettings(t))
		require.NoError(t, err)
	})
}

func TestParserProcess(t *testing.T) {
	cfg := NewConfig()
	cfg.OutputIDs = []string{"sink"}
	parser, err := cfg.Build(testTelemetrySettings(t))
	require.NoError(t, err)

	sink := stanzatest.NewSink()
	require.NoError(t, parser.SetOutputs([]operator.Operator{sink}))

	ent := entry.New()
	ent.Body = `{"level":"warn","msg":"hello","trace_id":"00000000000000000000000000000001","answer":42}`

	require.NoError(t, parser.Process(context.Background(), ent))

	require.Len(t, sink.Entries(), 1)
	got := sink.Entries()[0]
	require.Equal(t, "hello", got.Body)
	require.Equal(t, "warn", got.SeverityText)
	require.Equal(t, entry.Warn, got.Severity)
	require.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, got.TraceID)
	require.Equal(t, int64(42), got.Attributes["answer"])
	require.Equal(t, "generic-json", got.Attributes["logparser.type"])
}

func TestParserProcessDetectDisabled(t *testing.T) {
	cfg := NewConfig()
	cfg.Detect = false
	cfg.OutputIDs = []string{"sink"}
	parser, err := cfg.Build(testTelemetrySettings(t))
	require.NoError(t, err)

	sink := stanzatest.NewSink()
	require.NoError(t, parser.SetOutputs([]operator.Operator{sink}))

	ent := entry.New()
	ent.Body = `{"level":"warn","msg":"hello"}`

	require.NoError(t, parser.Process(context.Background(), ent))
	require.Len(t, sink.Entries(), 1)
	got := sink.Entries()[0]
	require.Equal(t, `{"level":"warn","msg":"hello"}`, got.Body)
	require.Empty(t, got.Attributes)
	require.Equal(t, entry.Default, got.Severity)
}

func TestSeverityMapping(t *testing.T) {
	tests := []struct {
		name   string
		stanza entry.Severity
		otlp   plog.SeverityNumber
	}{
		{name: "default", stanza: entry.Default, otlp: plog.SeverityNumberUnspecified},
		{name: "trace", stanza: entry.Trace, otlp: plog.SeverityNumberTrace},
		{name: "info", stanza: entry.Info, otlp: plog.SeverityNumberInfo},
		{name: "warn", stanza: entry.Warn, otlp: plog.SeverityNumberWarn},
		{name: "error", stanza: entry.Error, otlp: plog.SeverityNumberError},
		{name: "fatal4", stanza: entry.Fatal4, otlp: plog.SeverityNumberFatal4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.otlp, stanzaToOTLPSeverity(tt.stanza))
			require.Equal(t, tt.stanza, otlpToStanzaSeverity(tt.otlp))
		})
	}

	require.Equal(t, plog.SeverityNumberUnspecified, stanzaToOTLPSeverity(entry.Severity(1000)))
	require.Equal(t, entry.Default, otlpToStanzaSeverity(plog.SeverityNumber(1000)))
}

func TestApplyRecordParseToBodyKeepsParsedAttributesInBody(t *testing.T) {
	parseTo, err := entry.NewField("body")
	require.NoError(t, err)

	ent := entry.New()
	record := logparserRecord(t, "message")
	require.NoError(t, applyRecord(ent, parseTo, record))

	body, ok := ent.Body.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "bar", body["foo"])
	require.Equal(t, "generic-json", body["logparser.type"])
}

func TestApplyRecordMergesRootAttributesAndResource(t *testing.T) {
	parseTo := entry.NewAttributeField()
	ent := entry.New()
	ent.Attributes = map[string]any{"existing": "keep"}
	ent.Resource = map[string]any{"service.name": "api"}

	record := logparserRecord(t, "message")
	record.ResourceAttrs = otelstorage.NewAttrs()
	record.ResourceAttrs.AsMap().PutStr("service.namespace", "prod")

	require.NoError(t, applyRecord(ent, parseTo, record))
	require.Equal(t, "keep", ent.Attributes["existing"])
	require.Equal(t, "bar", ent.Attributes["foo"])
	require.Equal(t, "generic-json", ent.Attributes["logparser.type"])
	require.Equal(t, "api", ent.Resource["service.name"])
	require.Equal(t, "prod", ent.Resource["service.namespace"])
}

func TestRecordFromEntryDoesNotCopyExistingAttributes(t *testing.T) {
	ent := entry.New()
	ent.Body = "body"
	ent.Attributes = map[string]any{"existing": "value"}
	ent.Resource = map[string]any{"service.name": "api"}

	record := recordFromEntry(ent, "body")
	require.True(t, record.Attrs.IsZero())
	require.True(t, record.ResourceAttrs.IsZero())
}

func TestSetValueTypedSlices(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want []any
	}{
		{name: "strings", in: []string{"a", "b"}, want: []any{"a", "b"}},
		{name: "ints", in: []int{1, 2}, want: []any{int64(1), int64(2)}},
		{name: "int64s", in: []int64{1, 2}, want: []any{int64(1), int64(2)}},
		{name: "float64s", in: []float64{1.5, 2.5}, want: []any{1.5, 2.5}},
		{name: "bools", in: []bool{true, false}, want: []any{true, false}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := otelstorage.NewAttrs()
			setValue(attrs.AsMap().PutEmpty("value"), tt.in)
			got := mapFromAttrs(attrs)
			require.Equal(t, tt.want, got["value"])
		})
	}
}

func logparserRecord(t *testing.T, body string) logparser.Record {
	t.Helper()
	attrs := otelstorage.NewAttrs()
	attrs.AsMap().PutStr("foo", "bar")
	attrs.AsMap().PutStr("logparser.type", "generic-json")
	return logparser.Record{
		Body:           body,
		SeverityNumber: plog.SeverityNumberInfo,
		Attrs:          attrs,
	}
}

func BenchmarkParserProcess(b *testing.B) {
	b.ReportAllocs()

	cfg := NewConfig()
	cfg.OutputIDs = []string{"sink"}
	parser, err := cfg.Build(testTelemetrySettings(b))
	require.NoError(b, err)

	sink := stanzatest.NewSink()
	require.NoError(b, parser.SetOutputs([]operator.Operator{sink}))

	ctx := context.Background()
	for b.Loop() {
		ent := entry.New()
		ent.ObservedTimestamp = time.Unix(0, 1)
		ent.Body = `{"ts":"2026-06-10T12:00:00Z","level":"info","msg":"request complete","trace_id":"00000000000000000000000000000001","span_id":"0000000000000002","duration_ms":12.5,"ok":true}`
		if err := parser.Process(ctx, ent); err != nil {
			b.Fatal(err)
		}
		sink.Reset()
	}
}

func BenchmarkParserParseEntry(b *testing.B) {
	b.ReportAllocs()

	cfg := NewConfig()
	parser, err := cfg.Build(testTelemetrySettings(b))
	require.NoError(b, err)

	p := parser.(*Parser)
	for b.Loop() {
		ent := entry.New()
		ent.ObservedTimestamp = time.Unix(0, 1)
		ent.Body = `{"ts":"2026-06-10T12:00:00Z","level":"info","msg":"request complete","trace_id":"00000000000000000000000000000001","span_id":"0000000000000002","duration_ms":12.5,"ok":true}`
		if err := p.parseEntry(ent); err != nil {
			b.Fatal(err)
		}
	}
}

func testTelemetrySettings(tb testing.TB) component.TelemetrySettings {
	tb.Helper()
	return component.TelemetrySettings{Logger: zaptest.NewLogger(tb)}
}
