package otlpdirect_test

import (
	"testing"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// Protobuf puts no ordering on the fields of a message, and producers disagree: pdata's marshaler
// emits them descending (schema_url before scope), a hand-rolled encoder typically ascending. A
// decoder that consumes a submessage as it walks — rather than after — silently loses whichever
// sibling it already read, which is exactly how the scope schema_url went missing once.

// encodeScopeLogs builds one ExportLogsServiceRequest carrying a single record, writing each
// message's fields in the given order.
func encodeScopeLogs(ascending bool) []byte {
	var m easyproto.Marshaler

	req := m.MessageMarshaler()
	rl := req.AppendMessage(1) // resource_logs

	writeResource := func() {
		res := rl.AppendMessage(1) // resource
		kv := res.AppendMessage(1) // attributes
		kv.AppendString(1, "service.name")
		kv.AppendMessage(2).AppendString(1, "api")
	}

	writeScopes := func() {
		sl := rl.AppendMessage(2) // scope_logs

		writeScope := func() {
			sc := sl.AppendMessage(1) // scope
			sc.AppendString(1, "scope-name")
			sc.AppendString(2, "1.0.0")
		}

		writeRecords := func() {
			rec := sl.AppendMessage(2) // log_records
			rec.AppendFixed64(1, 1_700_000_000_000_000_000)
			rec.AppendInt64(2, 9)
			rec.AppendString(3, "INFO")
			rec.AppendMessage(5).AppendString(1, "body text") // body
		}

		writeSchema := func() { sl.AppendString(3, "https://schema.example/scope") }

		if ascending {
			writeScope()
			writeRecords()
			writeSchema()
		} else {
			writeSchema()
			writeRecords()
			writeScope()
		}
	}

	writeSchema := func() { rl.AppendString(3, "https://schema.example/resource") }

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

// TestConvertLogsIsFieldOrderIndependent pins that the two wire orderings decode identically.
func TestConvertLogsIsFieldOrderIndependent(t *testing.T) {
	t.Parallel()

	var asc, desc otlpdirect.LogsConverter

	up, err := asc.Convert(encodeScopeLogs(true))
	require.NoError(t, err)

	down, err := desc.Convert(encodeScopeLogs(false))
	require.NoError(t, err)

	require.Equal(t, canonical(down), canonical(up))

	// Spot-check the field the ordering bug actually dropped.
	sl := up.Resources[0].Scopes[0]
	assert.Equal(t, "https://schema.example/scope", string(sl.Scope.SchemaURL))
	assert.Equal(t, "scope-name", string(sl.Scope.Name))
	assert.Equal(t, "https://schema.example/resource", string(up.Resources[0].Resource.SchemaURL))
	assert.Equal(t, "body text", string(sl.Records[0].Body))
}
