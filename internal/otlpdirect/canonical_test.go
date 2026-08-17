package otlpdirect_test

import (
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
)

// The pdata path and the direct decoder disagree about how an absent byte field is spelled: pdata
// renders a missing string as a zero-length non-nil slice ([]byte("") is never nil) and a missing
// id as nil, while a decoder that simply never assigns the field leaves nil throughout.
//
// The distinction is not observable downstream. Identity hashing and column encoding both work off
// length, so nil and []byte{} produce the same bytes on disk and the same series id — which is why
// the parity tests canonicalize rather than chase pdata's spelling into the decoder. Everything
// else is compared exactly.

// canonical rewrites every zero-length byte slice in a batch to nil, so two batches that differ
// only in that spelling compare equal.
func canonical(l *log.Logs) *log.Logs {
	for i := range l.Resources {
		rl := &l.Resources[i]
		rl.Resource.SchemaURL = canonBytes(rl.Resource.SchemaURL)
		rl.Resource.Attributes = canonAttrs(rl.Resource.Attributes)

		for j := range rl.Scopes {
			sl := &rl.Scopes[j]
			sl.Scope.Name = canonBytes(sl.Scope.Name)
			sl.Scope.Version = canonBytes(sl.Scope.Version)
			sl.Scope.SchemaURL = canonBytes(sl.Scope.SchemaURL)
			sl.Scope.Attributes = canonAttrs(sl.Scope.Attributes)

			for k := range sl.Records {
				r := &sl.Records[k]
				r.SeverityText = canonBytes(r.SeverityText)
				r.Body = canonBytes(r.Body)
				r.TraceID = canonBytes(r.TraceID)
				r.SpanID = canonBytes(r.SpanID)
				r.Attributes = canonAttrs(r.Attributes)
			}
		}
	}

	return l
}

func canonBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}

	return b
}

func canonAttrs(a signal.Attributes) signal.Attributes {
	if len(a) == 0 {
		return nil
	}

	for i := range a {
		a[i].Key = canonBytes(a[i].Key)
		a[i].Value = canonValue(a[i].Value)
	}

	return a
}

func canonValue(v signal.Value) signal.Value {
	switch v.Kind() {
	case signal.KindStr:
		return signal.StringValue(canonBytes(v.Str()))
	case signal.KindBytes:
		return signal.BytesValue(canonBytes(v.Bytes()))
	case signal.KindSlice:
		vs := v.Slice()
		for i := range vs {
			vs[i] = canonValue(vs[i])
		}

		return signal.SliceValue(vs...)
	case signal.KindMap:
		return signal.MapValue(canonAttrs(v.Map())...)
	default:
		return v
	}
}
