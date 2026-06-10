package odbsafetyprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/oteldb/oteldb/odbagent/internal/odbsafety"
)

func TestLogsProcessorRedactsBelowLimit(t *testing.T) {
	now := time.Unix(100, 0)
	sink := &consumertest.LogsSink{}
	p := newTestProcessor(t, &Config{
		Config: odbsafety.Config{
			MaxRatePerSecond: 10,
			OnExcess:         odbsafety.ModeDrop,
			RedactFields:     []string{"password"},
		},
	}, sink, now)

	ld := newLogs("hello")
	record := firstRecord(ld)
	record.Attributes().PutStr("password", "secret")

	require.NoError(t, p.ConsumeLogs(context.Background(), ld))
	records := sinkRecords(t, sink)
	require.Len(t, records, 1)
	value, ok := records[0].Attributes().Get("password")
	require.True(t, ok)
	require.Equal(t, "<redacted>", value.Str())
}

func TestLogsProcessorDropExcess(t *testing.T) {
	now := time.Unix(100, 0)
	sink := &consumertest.LogsSink{}
	p := newTestProcessor(t, &Config{
		Config: odbsafety.Config{
			MaxRatePerSecond: 2,
			OnExcess:         odbsafety.ModeDrop,
		},
	}, sink, now)

	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs("a", "b", "c", "d")))
	records := sinkRecords(t, sink)
	requireBodies(t, records, []string{"a", "b"})
}

func TestLogsProcessorSampleExcess(t *testing.T) {
	tests := []struct {
		name       string
		sampleRate float64
		wantBodies []string
	}{
		{
			name:       "drop sampled-out excess",
			sampleRate: 0,
			wantBodies: []string{"a", "b"},
		},
		{
			name:       "keep sampled-in excess",
			sampleRate: 1,
			wantBodies: []string{"a", "b", "c", "d"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			sink := &consumertest.LogsSink{}
			p := newTestProcessor(t, &Config{
				Config: odbsafety.Config{
					MaxRatePerSecond: 2,
					OnExcess:         odbsafety.ModeSample,
					SampleRate:       tt.sampleRate,
				},
			}, sink, now)

			require.NoError(t, p.ConsumeLogs(context.Background(), newLogs("a", "b", "c", "d")))
			requireBodies(t, sinkRecords(t, sink), tt.wantBodies)
		})
	}
}

func TestLogsProcessorTruncateExcess(t *testing.T) {
	now := time.Unix(100, 0)
	sink := &consumertest.LogsSink{}
	p := newTestProcessor(t, &Config{
		Config: odbsafety.Config{
			MaxRatePerSecond: 1,
			OnExcess:         odbsafety.ModeTruncate,
			CompactWindow:    30 * time.Second,
		},
	}, sink, now)

	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs("a", "b", "c")))
	records := sinkRecords(t, sink)
	requireBodies(t, records, []string{"a", "<output is truncated>"})
	attrs := records[1].Attributes()
	truncated, ok := attrs.Get("oteldb.truncated_count")
	require.True(t, ok)
	require.Equal(t, int64(2), truncated.Int())
	windowStart, ok := attrs.Get("oteldb.window_start")
	require.True(t, ok)
	require.Equal(t, "1970-01-01T00:01:30Z", windowStart.Str())
}

func TestLogsProcessorCompactExcessByBody(t *testing.T) {
	now := time.Unix(100, 0)
	sink := &consumertest.LogsSink{}
	p := newTestProcessor(t, &Config{
		Config: odbsafety.Config{
			MaxRatePerSecond:  1,
			OnExcess:          odbsafety.ModeCompact,
			SampleRate:        0,
			CompactWindow:     30 * time.Second,
			CompactThreshold:  2,
			CompactMaxBuckets: 100,
		},
	}, sink, now)

	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs("same", "same", "same", "same")))
	records := sinkRecords(t, sink)
	requireBodies(t, records, []string{"same", "same", "same"})
	collapsed, ok := records[2].Attributes().Get("oteldb.collapsed_count")
	require.True(t, ok)
	require.Equal(t, int64(2), collapsed.Int())
}

func TestLogsProcessorCompactKeyFields(t *testing.T) {
	now := time.Unix(100, 0)
	sink := &consumertest.LogsSink{}
	p := newTestProcessor(t, &Config{
		Config: odbsafety.Config{
			MaxRatePerSecond:  1,
			OnExcess:          odbsafety.ModeCompact,
			SampleRate:        0,
			CompactWindow:     30 * time.Second,
			CompactThreshold:  1,
			CompactMaxBuckets: 100,
			CompactKeyFields:  []string{"body", "route"},
		},
	}, sink, now)

	ld := newLogs("same", "same", "same")
	records := allRecords(ld)
	records[0].Attributes().PutStr("route", "/a")
	records[1].Attributes().PutStr("route", "/a")
	records[2].Attributes().PutStr("route", "/b")

	require.NoError(t, p.ConsumeLogs(context.Background(), ld))
	got := sinkRecords(t, sink)
	require.Len(t, got, 3)
	_, ok := got[0].Attributes().Get("oteldb.collapsed_count")
	require.False(t, ok)
	for _, record := range got[1:] {
		collapsed, exists := record.Attributes().Get("oteldb.collapsed_count")
		require.True(t, exists)
		require.Equal(t, int64(1), collapsed.Int())
	}
}

func newTestProcessor(t *testing.T, cfg *Config, sink *consumertest.LogsSink, now time.Time) *logsProcessor {
	t.Helper()
	if cfg.OnExcess == "" {
		cfg.OnExcess = odbsafety.ModeConsume
	}
	p, err := newLogsProcessor(processor.Settings{
		TelemetrySettings: componentTelemetrySettings(),
	}, cfg, sink)
	require.NoError(t, err)
	p.now = func() time.Time { return now }
	return p
}

func componentTelemetrySettings() component.TelemetrySettings {
	return component.TelemetrySettings{MeterProvider: metricnoop.NewMeterProvider()}
}

func newLogs(bodies ...string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	for _, body := range bodies {
		record := sl.LogRecords().AppendEmpty()
		record.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(100, 0)))
		record.Body().SetStr(body)
	}
	return ld
}

func firstRecord(ld plog.Logs) plog.LogRecord {
	return ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
}

func allRecords(ld plog.Logs) []plog.LogRecord {
	records := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	out := make([]plog.LogRecord, 0, records.Len())
	for i := 0; i < records.Len(); i++ {
		out = append(out, records.At(i))
	}
	return out
}

func sinkRecords(t *testing.T, sink *consumertest.LogsSink) []plog.LogRecord {
	t.Helper()
	logs := sink.AllLogs()
	require.Len(t, logs, 1)
	return allRecords(logs[0])
}

func requireBodies(t *testing.T, records []plog.LogRecord, want []string) {
	t.Helper()
	got := make([]string, 0, len(records))
	for _, record := range records {
		got = append(got, record.Body().Str())
	}
	require.Equal(t, want, got)
}
