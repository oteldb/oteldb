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

	"github.com/oteldb/oteldb/internal/odbsafety"
)

func TestLogsProcessorRedactsBelowLimit(t *testing.T) {
	now := time.Unix(100, 0)
	sink := &consumertest.LogsSink{}
	p := newTestProcessor(t, &Config{
		Config: odbsafety.Config{
			SoftMaxRatePerSecond: 10,
			OnExcess:             odbsafety.ModeDrop,
			RedactFields:         []string{"password"},
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
			SoftMaxRatePerSecond: 2,
			OnExcess:             odbsafety.ModeDrop,
		},
	}, sink, now)

	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs("a", "b", "c", "d")))
	records := sinkRecords(t, sink)
	requireBodies(t, records, []string{"a", "b"})
}

func TestLogsProcessorSampleExcess(t *testing.T) {
	tests := []struct {
		name             string
		sampleFirst      int
		sampleThereafter int
		wantBodies       []string
	}{
		{
			name:             "drop sampled-out excess",
			sampleFirst:      0,
			sampleThereafter: 0,
			wantBodies:       []string{"a", "b"},
		},
		{
			name:             "keep sampled-in excess",
			sampleFirst:      100,
			sampleThereafter: 1,
			wantBodies:       []string{"a", "b", "c", "d"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			sink := &consumertest.LogsSink{}
			p := newTestProcessor(t, &Config{
				Config: odbsafety.Config{
					SoftMaxRatePerSecond: 2,
					OnExcess:             odbsafety.ModeSample,
					SampleFirst:          tt.sampleFirst,
					SampleThereafter:     tt.sampleThereafter,
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
			SoftMaxRatePerSecond: 1,
			OnExcess:             odbsafety.ModeTruncate,
			CompactWindow:        30 * time.Second,
		},
	}, sink, now)

	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs("a", "b", "c")))

	// Advance time to trigger flush
	p.now = func() time.Time { return time.Unix(200, 0) }
	p.handler.SetNow(p.now)
	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs())) // empty logs just to trigger flush

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

// TestLogsProcessorFlushOnEmptyBatch verifies that flushing pending
// synthetic records doesn't panic when a ConsumeLogs call carries no
// ResourceLogs/ScopeLogs to host them.
func TestLogsProcessorFlushOnEmptyBatch(t *testing.T) {
	now := time.Unix(100, 0)
	sink := &consumertest.LogsSink{}
	p := newTestProcessor(t, &Config{
		Config: odbsafety.Config{
			SoftMaxRatePerSecond: 1,
			OnExcess:             odbsafety.ModeTruncate,
			CompactWindow:        30 * time.Second,
		},
	}, sink, now)

	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs("a", "b", "c")))

	p.now = func() time.Time { return time.Unix(200, 0) }
	p.handler.SetNow(p.now)
	require.NoError(t, p.ConsumeLogs(context.Background(), plog.NewLogs()))

	records := sinkRecords(t, sink)
	requireBodies(t, records, []string{"a", "<output is truncated>"})
}

func TestLogsProcessorCompactExcessByBody(t *testing.T) {
	now := time.Unix(100, 0)
	sink := &consumertest.LogsSink{}
	p := newTestProcessor(t, &Config{
		Config: odbsafety.Config{
			SoftMaxRatePerSecond: 1,
			OnExcess:             odbsafety.ModeCompact,
			SampleFirst:          0,
			SampleThereafter:     0,
			CompactWindow:        30 * time.Second,
			CompactThreshold:     2,
			CompactMaxBuckets:    100,
		},
	}, sink, now)

	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs("same", "same", "same", "same")))

	// Advance time to trigger flush
	p.now = func() time.Time { return time.Unix(200, 0) }
	p.handler.SetNow(p.now)
	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs()))

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
			SoftMaxRatePerSecond: 1,
			OnExcess:             odbsafety.ModeCompact,
			SampleFirst:          0,
			SampleThereafter:     0,
			CompactWindow:        30 * time.Second,
			CompactThreshold:     1,
			CompactMaxBuckets:    100,
			CompactKeyFields:     []string{"body", "route"},
		},
	}, sink, now)

	ld := newLogs("same", "same", "same")
	records := allRecords(ld)
	records[0].Attributes().PutStr("route", "/a")
	records[1].Attributes().PutStr("route", "/a")
	records[2].Attributes().PutStr("route", "/b")

	require.NoError(t, p.ConsumeLogs(context.Background(), ld))

	// Advance time to trigger flush
	p.now = func() time.Time { return time.Unix(200, 0) }
	p.handler.SetNow(p.now)
	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs()))

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

func TestLogsProcessorConsumeMode(t *testing.T) {
	now := time.Unix(100, 0)
	sink := &consumertest.LogsSink{}
	p := newTestProcessor(t, &Config{
		Config: odbsafety.Config{
			SoftMaxRatePerSecond: 1,
			OnExcess:             odbsafety.ModeConsume,
		},
	}, sink, now)

	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs("a", "b", "c")))
	requireBodies(t, sinkRecords(t, sink), []string{"a", "b", "c"})
}

func TestLogsProcessorCompactEscalatesToTruncate(t *testing.T) {
	now := time.Unix(100, 0)
	sink := &consumertest.LogsSink{}
	p := newTestProcessor(t, &Config{
		Config: odbsafety.Config{
			SoftMaxRatePerSecond: 1,
			OnExcess:             odbsafety.ModeCompact,
			SampleFirst:          0,
			SampleThereafter:     0,
			CompactWindow:        30 * time.Second,
			CompactThreshold:     2,
			CompactMaxBuckets:    100,
			TruncateThreshold:    3,
		},
	}, sink, now)

	// "same" ×5: entry 1 passes; entries 2-5 are excess.
	// compact handler: count=1 passes, count=2 compacts, count=3 compacts, count=4 truncates.
	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs("same", "same", "same", "same", "same")))

	// Advance time to trigger flush
	p.now = func() time.Time { return time.Unix(200, 0) }
	p.handler.SetNow(p.now)
	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs()))

	records := sinkRecords(t, sink)
	requireBodies(t, records, []string{"same", "same", "<output is truncated>", "same"})
	truncated, ok := records[2].Attributes().Get("oteldb.truncated_count")
	require.True(t, ok)
	require.Equal(t, int64(1), truncated.Int())
	collapsed, ok := records[3].Attributes().Get("oteldb.collapsed_count")
	require.True(t, ok)
	require.Equal(t, int64(2), collapsed.Int())
}

func TestLogsProcessorCompactLRUOverflow(t *testing.T) {
	now := time.Unix(100, 0)
	sink := &consumertest.LogsSink{}
	p := newTestProcessor(t, &Config{
		Config: odbsafety.Config{
			SoftMaxRatePerSecond: 1,
			OnExcess:             odbsafety.ModeCompact,
			SampleFirst:          0,
			SampleThereafter:     0,
			CompactWindow:        30 * time.Second,
			CompactThreshold:     100, // high, so compact never fires
			CompactMaxBuckets:    2,
		},
	}, sink, now)

	// "a" passes (within limit); "x","y" are excess but count<threshold → pass;
	// "z" overflows LRU (maxBuckets=2) → degrades to sample (rate=0) → removed.
	require.NoError(t, p.ConsumeLogs(context.Background(), newLogs("a", "x", "y", "z")))
	requireBodies(t, sinkRecords(t, sink), []string{"a", "x", "y"})
}

func BenchmarkLogsProcessor(b *testing.B) {
	now := time.Unix(100, 0)
	benchmarks := []struct {
		name string
		cfg  odbsafety.Config
	}{
		{
			name: "disabled",
			cfg:  odbsafety.Config{},
		},
		{
			name: "drop-excess",
			cfg: odbsafety.Config{
				SoftMaxRatePerSecond: 1,
				OnExcess:             odbsafety.ModeDrop,
			},
		},
		{
			name: "compact-excess",
			cfg: odbsafety.Config{
				SoftMaxRatePerSecond: 1,
				OnExcess:             odbsafety.ModeCompact,
				CompactWindow:        30 * time.Second,
				CompactThreshold:     2,
				CompactMaxBuckets:    100,
			},
		},
		{
			name: "truncate-excess",
			cfg: odbsafety.Config{
				SoftMaxRatePerSecond: 1,
				OnExcess:             odbsafety.ModeTruncate,
				CompactWindow:        30 * time.Second,
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			sink := &consumertest.LogsSink{}
			p := newLogsProcessor(processor.Settings{
				TelemetrySettings: componentTelemetrySettings(),
			}, &Config{Config: bm.cfg}, sink)
			p.now = func() time.Time { return now }

			ctx := context.Background()
			for b.Loop() {
				if bm.name != "disabled" {
					p.windowStart = now
					p.windowCount = p.cfg.SoftLimit()
				}
				ld := newLogs("same")
				if err := p.ConsumeLogs(ctx, ld); err != nil {
					b.Fatal(err)
				}
				sink.Reset()
			}
		})
	}
}

func newTestProcessor(t *testing.T, cfg *Config, sink *consumertest.LogsSink, now time.Time) *logsProcessor {
	t.Helper()
	if cfg.OnExcess == "" {
		cfg.OnExcess = odbsafety.ModeConsume
	}
	p := newLogsProcessor(processor.Settings{
		TelemetrySettings: componentTelemetrySettings(),
	}, cfg, sink)
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
	if ld.ResourceLogs().Len() == 0 {
		return nil
	}
	if ld.ResourceLogs().At(0).ScopeLogs().Len() == 0 {
		return nil
	}
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
	var out []plog.LogRecord
	for _, l := range logs {
		out = append(out, allRecords(l)...)
	}
	return out
}

func requireBodies(t *testing.T, records []plog.LogRecord, want []string) {
	t.Helper()
	got := make([]string, 0, len(records))
	for _, record := range records {
		got = append(got, record.Body().Str())
	}
	require.Equal(t, want, got)
}
