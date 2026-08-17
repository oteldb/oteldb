package otlpdirect_test

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// FuzzConvertLogs drives the decoder with arbitrary bytes: the request is attacker-controlled, so
// no input may panic, hang, or allocate without bound — an error is the only acceptable failure.
//
// The seed corpus is real OTLP, so the fuzzer starts from structurally valid messages and mutates
// outward rather than spending its budget rediscovering the framing.
func FuzzConvertLogs(f *testing.F) {
	seed := func(build func(plog.Logs)) {
		ld := plog.NewLogs()
		build(ld)

		raw, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
		if err != nil {
			f.Fatal(err)
		}

		f.Add(raw)
	}

	seed(func(plog.Logs) {})
	seed(func(ld plog.Logs) {
		r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		r.SetTimestamp(1)
		r.Body().SetStr("hello")
	})
	seed(func(ld plog.Logs) {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.SetSchemaUrl("s")
		rl.Resource().Attributes().PutStr("service.name", "api")

		sl := rl.ScopeLogs().AppendEmpty()
		sl.SetSchemaUrl("scope")
		sl.Scope().SetName("n")

		r := sl.LogRecords().AppendEmpty()
		r.SetTimestamp(2)
		r.SetObservedTimestamp(3)
		r.SetSeverityNumber(plog.SeverityNumberWarn)
		r.SetSeverityText("WARN")
		r.SetTraceID([16]byte{1})
		r.SetSpanID([8]byte{2})
		r.SetFlags(plog.LogRecordFlags(1))
		r.SetDroppedAttributesCount(1)

		m := r.Body().SetEmptyMap()
		m.PutStr("k", "v")
		m.PutEmptySlice("l").AppendEmpty().SetInt(1)

		r.Attributes().PutEmptyBytes("b").Append(1, 2, 3)
		r.Attributes().PutDouble("d", 1.5)
	})

	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		var c otlpdirect.LogsConverter

		got, err := c.Convert(data)
		if err != nil {
			return
		}

		// A successful decode must be internally consistent: every slot the walk created is
		// reachable and every record is readable without panicking.
		for i := range got.Resources {
			rl := &got.Resources[i]
			_ = len(rl.Resource.SchemaURL)

			for j := range rl.Scopes {
				sl := &rl.Scopes[j]
				_ = len(sl.Scope.Name)

				for k := range sl.Records {
					rec := &sl.Records[k]
					_ = len(rec.Body) + len(rec.TraceID) + len(rec.SpanID) + len(rec.Attributes)
				}
			}
		}

		// Reuse must not corrupt the previous batch's scratch into the next one.
		if _, err := c.Convert(data); err != nil {
			t.Fatalf("second convert of the same input failed: %v", err)
		}
	})
}
