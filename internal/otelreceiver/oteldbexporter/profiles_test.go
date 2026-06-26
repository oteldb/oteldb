package oteldbexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/pdata/pprofile"
)

type fakeProfilesSink struct {
	consumed int
}

func (s *fakeProfilesSink) ConsumeProfiles(context.Context, pprofile.Profiles) error {
	s.consumed++
	return nil
}

// TestProfilesSinkRouting verifies that configuring a profiles sink makes the exporter advertise
// the experimental profiles signal and route ingested profiles to the sink.
func TestProfilesSinkRouting(t *testing.T) {
	sink := &fakeProfilesSink{}
	f := NewFactory(WithProfilesSink(sink))

	xf, ok := f.(xexporter.Factory)
	require.True(t, ok, "factory must support the experimental profiles signal")

	ctx := context.Background()
	exp, err := xf.CreateProfiles(ctx, exportertest.NewNopSettings(f.Type()), createDefaultConfig())
	require.NoError(t, err)

	require.NoError(t, exp.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { _ = exp.Shutdown(ctx) })

	require.NoError(t, exp.ConsumeProfiles(ctx, pprofile.NewProfiles()))
	require.Equal(t, 1, sink.consumed)
}

// TestProfilesUnsupportedWithoutSink verifies that without a profiles sink the exporter does not
// advertise the profiles signal.
func TestProfilesUnsupportedWithoutSink(t *testing.T) {
	f := NewFactory()
	if xf, ok := f.(xexporter.Factory); ok {
		_, err := xf.CreateProfiles(context.Background(), exportertest.NewNopSettings(f.Type()), createDefaultConfig())
		require.Error(t, err, "profiles must be unsupported without a sink")
	}
}

var _ exporter.Factory = NewFactory()
