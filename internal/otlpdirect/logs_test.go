package otlpdirect_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal/log"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// marshal serializes a plog.Logs as an ExportLogsServiceRequest — the bytes a real OTLP client
// puts on the wire.
func marshal(tb testing.TB, ld plog.Logs) []byte {
	tb.Helper()

	raw, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	require.NoError(tb, err)

	return raw
}

// convertBoth decodes src directly and via the pdata path, returning both batches for comparison.
func convertBoth(tb testing.TB, ld plog.Logs) (direct, viaPdata *log.Logs) {
	tb.Helper()

	var c otlpdirect.LogsConverter

	direct, err := c.Convert(marshal(tb, ld))
	require.NoError(tb, err)

	viaPdata = &log.Logs{}
	require.Zero(tb, pdataconv.AppendLogs(viaPdata, ld))

	return canonical(direct), canonical(viaPdata)
}

// TestConvertLogsMatchesPdata is the contract: decoding the wire bytes directly must produce
// exactly what the receiver → pdata → pdataconv path produces. Anything else is a silent
// divergence between two ingest paths for the same request.
func TestConvertLogsMatchesPdata(t *testing.T) {
	t.Parallel()

	ld := plog.NewLogs()

	rl := ld.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://schema.example/1.0")
	rl.Resource().Attributes().PutStr("service.name", "api")
	rl.Resource().Attributes().PutInt("service.instance", 7)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://schema.example/scope")
	sl.Scope().SetName("go.opentelemetry.io/example")
	sl.Scope().SetVersion("1.2.3")
	sl.Scope().Attributes().PutBool("experimental", true)

	r := sl.LogRecords().AppendEmpty()
	r.SetTimestamp(1_700_000_000_000_000_000)
	r.SetObservedTimestamp(1_700_000_000_000_000_001)
	r.SetSeverityNumber(plog.SeverityNumberError)
	r.SetSeverityText("ERROR")
	r.Body().SetStr("connection refused")
	r.SetFlags(plog.LogRecordFlags(1))
	r.SetDroppedAttributesCount(3)
	r.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	r.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	r.Attributes().PutStr("host", "node-1")
	r.Attributes().PutDouble("latency", 12.5)
	r.Attributes().PutBool("retried", false)
	r.Attributes().PutEmptyBytes("payload").Append(0xde, 0xad)

	direct, viaPdata := convertBoth(t, ld)
	assert.Equal(t, viaPdata, direct)
}

// TestConvertLogsBodyKinds pins the body rendering: the model stores a body as text, so every
// non-string AnyValue must render exactly as the pdata path renders it.
func TestConvertLogsBodyKinds(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		set  func(pcommon.Value)
		want string
	}{
		{"string", func(v pcommon.Value) { v.SetStr("plain") }, "plain"},
		{"bool", func(v pcommon.Value) { v.SetBool(true) }, "true"},
		{"int", func(v pcommon.Value) { v.SetInt(-42) }, "-42"},
		{"double", func(v pcommon.Value) { v.SetDouble(1.5) }, "1.5"},
		{"empty", func(pcommon.Value) {}, ""},
		{"bytes", func(v pcommon.Value) { v.SetEmptyBytes().Append(0xde, 0xad) }, "3q0="},
		{"slice", func(v pcommon.Value) {
			s := v.SetEmptySlice()
			s.AppendEmpty().SetInt(1)
			s.AppendEmpty().SetStr("two")
		}, `[1,"two"]`},
		{"map", func(v pcommon.Value) { v.SetEmptyMap().PutStr("k", "v") }, `{"k":"v"}`},
		{"html in string", func(v pcommon.Value) { v.SetEmptyMap().PutStr("k", "a<b&c") }, `{"k":"a<b&c"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ld := plog.NewLogs()
			r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			r.SetTimestamp(1)
			tt.set(r.Body())

			direct, viaPdata := convertBoth(t, ld)
			require.Equal(t, viaPdata, direct)

			got := direct.Resources[0].Scopes[0].Records[0].Body
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// TestConvertLogsNestedAttributes covers the recursive value kinds, where a decoder is easiest to
// get subtly wrong.
func TestConvertLogsNestedAttributes(t *testing.T) {
	t.Parallel()

	ld := plog.NewLogs()
	r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	r.SetTimestamp(1)

	m := r.Attributes().PutEmptyMap("nested")
	m.PutStr("inner", "value")
	m.PutInt("count", 2)

	inner := m.PutEmptySlice("list")
	inner.AppendEmpty().SetStr("a")
	inner.AppendEmpty().SetDouble(0.5)

	// Keys deliberately out of order: the decoder must sort them, or identity hashes diverge.
	r.Attributes().PutStr("zeta", "last")
	r.Attributes().PutStr("alpha", "first")

	direct, viaPdata := convertBoth(t, ld)
	assert.Equal(t, viaPdata, direct)
}

// TestConvertLogsMultipleResourcesAndScopes pins the hierarchy walk.
func TestConvertLogsMultipleResourcesAndScopes(t *testing.T) {
	t.Parallel()

	ld := plog.NewLogs()

	for i := range 3 {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutInt("idx", int64(i))

		for j := range 2 {
			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName("scope")
			sl.Scope().SetVersion(string(rune('a' + j)))

			for k := range 4 {
				r := sl.LogRecords().AppendEmpty()
				r.SetTimestamp(pcommon.Timestamp(1_000 + k))
				r.Body().SetStr("body")
			}
		}
	}

	direct, viaPdata := convertBoth(t, ld)
	require.Equal(t, viaPdata, direct)

	assert.Len(t, direct.Resources, 3)
	assert.Len(t, direct.Resources[0].Scopes, 2)
	assert.Len(t, direct.Resources[0].Scopes[0].Records, 4)
}

// TestConvertLogsEmpty covers the degenerate inputs a real sender does produce.
func TestConvertLogsEmpty(t *testing.T) {
	t.Parallel()

	var c otlpdirect.LogsConverter

	got, err := c.Convert(nil)
	require.NoError(t, err)
	assert.Empty(t, got.Resources)

	direct, viaPdata := convertBoth(t, plog.NewLogs())
	assert.Equal(t, viaPdata, direct)
}

// TestConvertLogsReuseIsIsolated pins that a converter reused across requests does not leak the
// previous batch's contents — the risk of retaining scratch across calls.
func TestConvertLogsReuseIsIsolated(t *testing.T) {
	t.Parallel()

	var c otlpdirect.LogsConverter

	first := plog.NewLogs()
	fr := first.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	fr.SetTimestamp(1)
	fr.Body().SetStr("first")
	fr.Attributes().PutStr("a", "1")

	second := plog.NewLogs()
	sr := second.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	sr.SetTimestamp(2)
	sr.Body().SetStr("second")
	sr.Attributes().PutStr("b", "2")

	_, err := c.Convert(marshal(t, first))
	require.NoError(t, err)

	got, err := c.Convert(marshal(t, second))
	require.NoError(t, err)

	want := &log.Logs{}
	require.Zero(t, pdataconv.AppendLogs(want, second))
	assert.Equal(t, canonical(want), canonical(got))
}

func BenchmarkConvertLogs(b *testing.B) {
	ld := plog.NewLogs()

	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("bench")

	for i := range 1_000 {
		r := sl.LogRecords().AppendEmpty()
		r.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + i))
		r.SetSeverityNumber(plog.SeverityNumberInfo)
		r.SetSeverityText("INFO")
		r.Body().SetStr("request completed")
		r.Attributes().PutStr("http.route", "/api/v1/things")
		r.Attributes().PutInt("http.status_code", 200)
	}

	raw := marshal(b, ld)

	b.Run("Direct", func(b *testing.B) {
		var c otlpdirect.LogsConverter

		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))

		for b.Loop() {
			if _, err := c.Convert(raw); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Pdata", func(b *testing.B) {
		var u plog.ProtoUnmarshaler

		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))

		for b.Loop() {
			decoded, err := u.UnmarshalLogs(raw)
			if err != nil {
				b.Fatal(err)
			}

			dst := &log.Logs{}
			pdataconv.AppendLogs(dst, decoded)
		}
	})
}
