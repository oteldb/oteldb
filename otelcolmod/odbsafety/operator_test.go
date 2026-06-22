package odbsafety

import (
	"context"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap/zaptest"

	safetyconfig "github.com/oteldb/oteldb/internal/odbsafety"
	"github.com/oteldb/oteldb/internal/stanzatest"
)

func TestConfigBuild(t *testing.T) {
	t.Run("registered", func(t *testing.T) {
		builder, ok := operator.Lookup(Type)
		require.True(t, ok)
		require.IsType(t, &Config{}, builder())
	})

	t.Run("invalid", func(t *testing.T) {
		cfg := NewConfig()
		cfg.OnExcess = "missing"

		_, err := cfg.Build(testTelemetrySettings(t))
		require.EqualError(t, err, `on_excess must be one of consume, drop, sample, compact, truncate, got "missing"`)
	})
}

func TestTransformerDropExcess(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	sink := stanzatest.NewSink()
	transformer := newTestTransformer(t, &Config{
		Config: safetyconfig.Config{
			SoftMaxRatePerSecond: 2,
			OnExcess:             safetyconfig.ModeDrop,
		},
	}, sink, now)

	require.NoError(t, transformer.ProcessBatch(context.Background(), entries("a", "b", "c", "d")))
	requireBodies(t, sink.Entries(), []string{"a", "b"})
}

func TestTransformerRedactsBelowLimit(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	sink := stanzatest.NewSink()
	transformer := newTestTransformer(t, &Config{
		Config: safetyconfig.Config{
			SoftMaxRatePerSecond: 10,
			OnExcess:             safetyconfig.ModeDrop,
			RedactFields:         []string{"password"},
		},
	}, sink, now)

	ents := entries("a")
	ents[0].Attributes = map[string]any{"password": "secret"}
	require.NoError(t, transformer.ProcessBatch(context.Background(), ents))
	require.Equal(t, "<redacted>", sink.Entries()[0].Attributes["password"])
}

func TestTransformerTruncateExcess(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	sink := stanzatest.NewSink()
	transformer := newTestTransformer(t, &Config{
		Config: safetyconfig.Config{
			SoftMaxRatePerSecond: 1,
			OnExcess:             safetyconfig.ModeTruncate,
			CompactWindow:        30 * time.Second,
		},
	}, sink, now)

	require.NoError(t, transformer.ProcessBatch(context.Background(), entries("a", "b", "c")))

	transformer.now = func() time.Time { return time.Unix(200, 0).UTC() }
	transformer.handler.SetNow(transformer.now)
	require.NoError(t, transformer.ProcessBatch(context.Background(), entries()))

	got := sink.Entries()
	requireBodies(t, got, []string{"a", "<output is truncated>"})
	require.Equal(t, int64(2), got[1].Attributes["oteldb.truncated_count"])
	require.Equal(t, "1970-01-01T00:01:30Z", got[1].Attributes["oteldb.window_start"])
}

func TestTransformerCompactExcess(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	sink := stanzatest.NewSink()
	transformer := newTestTransformer(t, &Config{
		Config: safetyconfig.Config{
			SoftMaxRatePerSecond: 1,
			OnExcess:             safetyconfig.ModeCompact,
			SampleFirst:          0,
			SampleThereafter:     0,
			CompactWindow:        30 * time.Second,
			CompactThreshold:     2,
			CompactMaxBuckets:    100,
		},
	}, sink, now)

	require.NoError(t, transformer.ProcessBatch(context.Background(), entries("same", "same", "same", "same")))

	transformer.now = func() time.Time { return time.Unix(200, 0).UTC() }
	transformer.handler.SetNow(transformer.now)
	require.NoError(t, transformer.ProcessBatch(context.Background(), entries()))

	got := sink.Entries()
	requireBodies(t, got, []string{"same", "same", "same"})
	require.Equal(t, int64(2), got[2].Attributes["oteldb.collapsed_count"])
}

func TestTransformerConsumeMode(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	sink := stanzatest.NewSink()
	transformer := newTestTransformer(t, &Config{
		Config: safetyconfig.Config{
			SoftMaxRatePerSecond: 1,
			OnExcess:             safetyconfig.ModeConsume,
		},
	}, sink, now)

	require.NoError(t, transformer.ProcessBatch(context.Background(), entries("a", "b", "c")))
	requireBodies(t, sink.Entries(), []string{"a", "b", "c"})
}

func TestTransformerSampleExcess(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	sink := stanzatest.NewSink()
	transformer := newTestTransformer(t, &Config{
		Config: safetyconfig.Config{
			SoftMaxRatePerSecond: 2,
			OnExcess:             safetyconfig.ModeSample,
			SampleFirst:          0,
			SampleThereafter:     0,
		},
	}, sink, now)

	require.NoError(t, transformer.ProcessBatch(context.Background(), entries("a", "b", "c", "d")))
	requireBodies(t, sink.Entries(), []string{"a", "b"})
}

func TestTransformerCompactEscalatesToTruncate(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	sink := stanzatest.NewSink()
	transformer := newTestTransformer(t, &Config{
		Config: safetyconfig.Config{
			SoftMaxRatePerSecond: 1,
			OnExcess:             safetyconfig.ModeCompact,
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
	require.NoError(t, transformer.ProcessBatch(context.Background(), entries("same", "same", "same", "same", "same")))

	transformer.now = func() time.Time { return time.Unix(200, 0).UTC() }
	transformer.handler.SetNow(transformer.now)
	require.NoError(t, transformer.ProcessBatch(context.Background(), entries()))

	got := sink.Entries()
	requireBodies(t, got, []string{"same", "same", "<output is truncated>", "same"})
	require.Equal(t, int64(1), got[2].Attributes["oteldb.truncated_count"])
	require.Equal(t, int64(2), got[3].Attributes["oteldb.collapsed_count"])
}

func TestTransformerCompactLRUOverflow(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	sink := stanzatest.NewSink()
	transformer := newTestTransformer(t, &Config{
		Config: safetyconfig.Config{
			SoftMaxRatePerSecond: 1,
			OnExcess:             safetyconfig.ModeCompact,
			SampleFirst:          0,
			SampleThereafter:     0,
			CompactWindow:        30 * time.Second,
			CompactThreshold:     100, // high, so compact never fires
			CompactMaxBuckets:    2,
		},
	}, sink, now)

	// "a" passes (within limit); "x","y" are excess but count<threshold → pass;
	// "z" overflows LRU (maxBuckets=2) → degrades to sample (rate=0) → dropped.
	require.NoError(t, transformer.ProcessBatch(context.Background(), entries("a", "x", "y", "z")))
	requireBodies(t, sink.Entries(), []string{"a", "x", "y"})
}

func newTestTransformer(t *testing.T, cfg *Config, sink operator.Operator, now time.Time) *Transformer {
	t.Helper()
	cfg.TransformerConfig = NewConfig().TransformerConfig
	cfg.OutputIDs = []string{"sink"}
	op, err := cfg.Build(testTelemetrySettings(t))
	require.NoError(t, err)
	require.NoError(t, op.SetOutputs([]operator.Operator{sink}))
	transformer := op.(*Transformer)
	transformer.now = func() time.Time { return now }
	return transformer
}

func BenchmarkTransformerProcess(b *testing.B) {
	now := time.Unix(100, 0).UTC()
	benchmarks := []struct {
		name string
		cfg  safetyconfig.Config
	}{
		{
			name: "disabled",
			cfg:  safetyconfig.Config{},
		},
		{
			name: "drop-excess",
			cfg: safetyconfig.Config{
				SoftMaxRatePerSecond: 1,
				OnExcess:             safetyconfig.ModeDrop,
			},
		},
		{
			name: "compact-excess",
			cfg: safetyconfig.Config{
				SoftMaxRatePerSecond: 1,
				OnExcess:             safetyconfig.ModeCompact,
				SampleFirst:          0,
				SampleThereafter:     0,
				CompactWindow:        30 * time.Second,
				CompactThreshold:     2,
				CompactMaxBuckets:    100,
			},
		},
		{
			name: "truncate-excess",
			cfg: safetyconfig.Config{
				SoftMaxRatePerSecond: 1,
				OnExcess:             safetyconfig.ModeTruncate,
				CompactWindow:        30 * time.Second,
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			transformer := &Transformer{
				cfg:          bm.cfg,
				redactFields: bm.cfg.RedactFields,
				handler:      safetyconfig.NewHandler[*entry.Entry](bm.cfg, func() bool { return false }, safetyconfig.NoopMetrics{}),
				now:          func() time.Time { return now },
			}
			ctx := context.Background()
			for b.Loop() {
				if bm.name != "disabled" {
					transformer.rateWindowStart = now
					transformer.rateWindowCount = transformer.cfg.SoftLimit()
				}
				out := make([]*entry.Entry, 0, 1)
				batch := processBatch{ctx: ctx, output: &out}
				ent := entry.New()
				ent.Timestamp = now
				ent.Body = "same"
				if err := transformer.process(ctx, ent, &batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func entries(bodies ...string) []*entry.Entry {
	out := make([]*entry.Entry, 0, len(bodies))
	for _, body := range bodies {
		ent := entry.New()
		ent.Timestamp = time.Unix(100, 0).UTC()
		ent.Body = body
		out = append(out, ent)
	}
	return out
}

func requireBodies(t *testing.T, entries []*entry.Entry, want []string) {
	t.Helper()
	got := make([]string, 0, len(entries))
	for _, ent := range entries {
		got = append(got, ent.Body.(string))
	}
	require.Equal(t, want, got)
}

func testTelemetrySettings(tb testing.TB) component.TelemetrySettings {
	tb.Helper()
	return component.TelemetrySettings{Logger: zaptest.NewLogger(tb)}
}
