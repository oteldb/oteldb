package logqlabels

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

func attrs(kv ...string) otelstorage.Attrs {
	m := pcommon.NewMap()
	for i := 0; i+1 < len(kv); i += 2 {
		m.PutStr(kv[i], kv[i+1])
	}
	return otelstorage.Attrs(m)
}

func getStr(t *testing.T, l *LabelSet, name string) (string, bool) {
	t.Helper()
	v, ok := l.Get(logql.Label(name))
	if !ok {
		return "", false
	}
	return v.AsString(), true
}

func TestSetFromRecord_ServiceNameDefault(t *testing.T) {
	l := NewLabelSet()

	// No service.name resource attribute -> default "unknown_service" (Loki parity).
	l.SetFromRecord(logstorage.Record{ResourceAttrs: attrs("otelbench.resource", "9")})
	v, ok := getStr(t, &l, logstorage.LabelServiceName)
	require.True(t, ok, "service_name must be set by default")
	assert.Equal(t, "unknown_service", v)

	// An explicit service.name resource attribute wins over the default.
	l.SetFromRecord(logstorage.Record{ResourceAttrs: attrs("service.name", "api")})
	v, ok = getStr(t, &l, logstorage.LabelServiceName)
	require.True(t, ok)
	assert.Equal(t, "api", v)
}

func TestSetFromRecord_LevelUppercase(t *testing.T) {
	// SeverityNumber path.
	l := NewLabelSet()
	l.SetFromRecord(logstorage.Record{SeverityNumber: plog.SeverityNumberError})
	for _, label := range []string{logstorage.LabelSeverity, logstorage.LabelDetectedLevel} {
		v, ok := getStr(t, &l, label)
		require.True(t, ok, label)
		assert.Equal(t, "ERROR", v, label)
	}

	// SeverityText path (number unspecified) is also normalized to upper-case.
	l.SetFromRecord(logstorage.Record{SeverityText: "warn"})
	v, ok := getStr(t, &l, logstorage.LabelSeverity)
	require.True(t, ok)
	assert.Equal(t, "WARN", v)
}
