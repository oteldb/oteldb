package otlpdirect_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func marshal(tb testing.TB, data plog.Logs) []byte {
	tb.Helper()
	raw, err := (&plog.ProtoMarshaler{}).MarshalLogs(data)
	require.NoError(tb, err)
	return raw
}

func marshalTraces(tb testing.TB, data ptrace.Traces) []byte {
	tb.Helper()
	raw, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(data)
	require.NoError(tb, err)
	return raw
}

func marshalMetrics(tb testing.TB, data pmetric.Metrics) []byte {
	tb.Helper()
	raw, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(data)
	require.NoError(tb, err)
	return raw
}
