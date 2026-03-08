package oteldbexporter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func makeLogs(resources, scopes, records int) plog.Logs {
	ld := plog.NewLogs()
	for range resources {
		rl := ld.ResourceLogs().AppendEmpty()
		for range scopes {
			sl := rl.ScopeLogs().AppendEmpty()
			for range records {
				sl.LogRecords().AppendEmpty()
			}
		}
	}
	return ld
}

func TestSampleLogRecords_DropAll(t *testing.T) {
	ld := makeLogs(2, 3, 4)
	require.Equal(t, 24, ld.LogRecordCount())

	sampleLogRecords(ld, SamplingConfig{Drop: true})

	require.Equal(t, 0, ld.LogRecordCount())
	// Empty ResourceLogs must be pruned.
	require.Equal(t, 0, ld.ResourceLogs().Len())
}

func TestSampleLogRecords_Disabled(t *testing.T) {
	ld := makeLogs(2, 3, 4)
	sampleLogRecords(ld, SamplingConfig{})
	require.Equal(t, 24, ld.LogRecordCount())
	require.Equal(t, 2, ld.ResourceLogs().Len())
}

func TestSampleLogRecords_RateOne(t *testing.T) {
	ld := makeLogs(2, 3, 4)
	sampleLogRecords(ld, SamplingConfig{Rate: 1.0})
	require.Equal(t, 24, ld.LogRecordCount())
}

func TestSampleLogRecords_RateSampling(t *testing.T) {
	const n = 2000
	ld := makeLogs(1, 1, n)
	sampleLogRecords(ld, SamplingConfig{Rate: 0.5})
	got := ld.LogRecordCount()
	// Expect ~50%, allow ±15%.
	require.InDelta(t, n/2, got, n*0.15)
}

func TestSampleLogRecords_PrunesEmptyScopeLogs(t *testing.T) {
	// 1 resource, 2 scopes, each with 1 record. Drop all → both scopes and resource pruned.
	ld := makeLogs(1, 2, 1)
	sampleLogRecords(ld, SamplingConfig{Drop: true})
	require.Equal(t, 0, ld.ResourceLogs().Len())
}
