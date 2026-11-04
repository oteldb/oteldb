package storagebackend

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-faster/sdk/app"
	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func TestRemovedRetentionKey(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"null", "{}", "{log: 1GB}", "0", "[]"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			var cfg RetentionConfig
			err := yaml.Unmarshal([]byte("max_bytes_per_signal: "+value), &cfg)
			require.ErrorContains(t, err, "retention.signals.<signal>.max_bytes")
		})
	}
	for _, data := range []string{`{"max_bytes_per_signal":null}`, `{"max_bytes_per_signal":{}}`, `{"max_bytes_per_signal":{"log":"1GB"}}`} {
		var cfg RetentionConfig
		require.ErrorContains(t, json.Unmarshal([]byte(data), &cfg), "retention.signals.<signal>.max_bytes")
	}
}

func FuzzRemovedRetentionKey(f *testing.F) {
	for _, s := range []string{"null", "{}", "{log: 1GB}", "0", "[]"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 4096 {
			t.Skip()
		}
		var cfg RetentionConfig
		require.Error(t, yaml.Unmarshal([]byte("max_bytes_per_signal: "+value), &cfg))
	})
}

func TestRetentionSignalPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		cfg  RetentionConfig
		err  bool
		age  time.Duration
	}{
		{name: "longer", cfg: RetentionConfig{MaxAge: 24 * time.Hour, Signals: map[string]SignalRetentionConfig{"metric": {MaxAge: 72 * time.Hour}}}, age: 72 * time.Hour},
		{name: "shorter", cfg: RetentionConfig{MaxAge: 24 * time.Hour, Signals: map[string]SignalRetentionConfig{"metric": {MaxAge: time.Hour}}}, age: time.Hour},
		{name: "inherit", cfg: RetentionConfig{MaxAge: 24 * time.Hour, Signals: map[string]SignalRetentionConfig{"metric": {}}}, age: 24 * time.Hour},
		{name: "negative default age", cfg: RetentionConfig{MaxAge: -time.Hour}, err: true},
		{name: "negative signal age", cfg: RetentionConfig{Signals: map[string]SignalRetentionConfig{"log": {MaxAge: -time.Hour}}}, err: true},
		{name: "negative signal bytes", cfg: RetentionConfig{Signals: map[string]SignalRetentionConfig{"log": {MaxBytes: negativeBytes}}}, err: true},
		{name: "budget above pool", cfg: RetentionConfig{MaxBytes: 10, Signals: map[string]SignalRetentionConfig{"log": {MaxBytes: 11}}}, err: true},
		{name: "unknown", cfg: RetentionConfig{Signals: map[string]SignalRetentionConfig{"logs": {MaxAge: time.Hour}}}, err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opt, err := tenancyOption(&PolicyConfig{Retention: &tc.cfg})
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			resolved := applyOption(t, opt).Tenancy.Resolve("default").Retention
			require.Equal(t, tc.age, resolved.AgeFor(signal.Metric))
		})
	}
}

func TestRetentionWarnings(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		cfg  *PolicyConfig
		ret  tenant.Retention
		want int
	}{
		{name: "unbounded", ret: tenant.Retention{}, want: 0},
		{name: "same ages", ret: tenant.Retention{MaxAge: time.Hour}, want: 0},
		{name: "short exemplars", ret: tenant.Retention{MaxAge: time.Hour, PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Exemplar: {MaxAge: time.Minute}}}, want: 0},
		{name: "long exemplars", ret: tenant.Retention{MaxAge: time.Hour, PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Exemplar: {MaxAge: 2 * time.Hour}}}, want: 1},
		{name: "unbounded exemplars", ret: tenant.Retention{PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Trace: {MaxAge: time.Hour}}}, want: 1},
		{name: "inactive tiers", cfg: &PolicyConfig{Precision: []PrecisionTierConfig{{After: time.Hour, Bits: 32}}, Downsample: []DownsampleTierConfig{{After: 2 * time.Hour, Interval: time.Minute}}, Recompress: &RecompressConfig{After: time.Hour, Level: 9}, EC: &ECConfig{After: 2 * time.Hour}}, ret: tenant.Retention{MaxAge: time.Hour}, want: 4},
		{name: "disabled tiers", cfg: &PolicyConfig{Precision: []PrecisionTierConfig{{After: time.Hour}}, Downsample: []DownsampleTierConfig{{After: 2 * time.Hour}}}, ret: tenant.Retention{MaxAge: time.Hour}, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			core, logs := observer.New(zap.WarnLevel)
			warnRetentionPolicy(zap.New(core), tc.cfg, tc.ret)
			require.Equal(t, tc.want, logs.Len())
		})
	}
}

func TestOpenRetentionAdmissionAndMetadata(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		name := "default-off"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{Backend: "memory", RejectOutOfRetention: enabled, Policy: &PolicyConfig{Retention: &RetentionConfig{MaxAge: 24 * time.Hour, Signals: map[string]SignalRetentionConfig{"log": {MaxAge: time.Hour, MaxBytes: 1000}}}}}
			core, logs := observer.New(zap.InfoLevel)
			b, closeStore, err := Open(t.Context(), cfg, zap.New(core), &app.Telemetry{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, closeStore(context.Background())) })
			policy, known := b.RetentionPolicy("default")
			require.True(t, known)
			require.Equal(t, time.Hour, policy.AgeFor(signal.Log))
			require.Equal(t, int64(1000), policy.PerSignal[signal.Log].MaxBytes)
			require.Equal(t, enabled, b.RejectOutOfRetention())
			policy.PerSignal[signal.Log] = tenant.SignalRetention{MaxAge: time.Nanosecond}
			again, _ := b.RetentionPolicy("default")
			require.Equal(t, time.Hour, again.AgeFor(signal.Log), "metadata cannot mutate live policy")
			require.Equal(t, 5, logs.FilterMessage("Effective embedded storage retention").Len())
			ld := plog.NewLogs()
			records := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
			for _, age := range []time.Duration{2 * time.Hour, time.Minute} {
				rec := records.AppendEmpty()
				rec.SetTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(-age)))
				rec.Body().SetStr("body")
			}
			require.NoError(t, b.ConsumeLogs(t.Context(), ld))
			stats := b.Inspect()
			require.Len(t, stats.Tenants, 1)
			wantAccepted, wantRejected := int64(2), int64(0)
			if enabled {
				wantAccepted, wantRejected = 1, 1
			}
			require.Equal(t, wantAccepted, stats.Tenants[0].Admission.Accepted)
			require.Equal(t, wantRejected, stats.Tenants[0].Admission.RejectedRetention)
		})
	}
}

func TestRetentionMetadataUnknownAndUnbounded(t *testing.T) {
	t.Parallel()
	_, known := NewQuery(nil).RetentionPolicy("default")
	require.False(t, known)
	b, stop, err := Open(t.Context(), Config{Backend: "memory"}, zap.NewNop(), &app.Telemetry{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stop(context.Background())) })
	p, known := b.RetentionPolicy("default")
	require.True(t, known)
	require.Zero(t, p.AgeFor(signal.Log))
	require.False(t, b.RejectOutOfRetention())
}
