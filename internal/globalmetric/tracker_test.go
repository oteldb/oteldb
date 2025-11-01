package globalmetric

import (
	"context"
	"testing"

	"github.com/ClickHouse/ch-go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	noopmeter "go.opentelemetry.io/otel/metric/noop"
	nooptracer "go.opentelemetry.io/otel/trace/noop"
)

func TestTracker(t *testing.T) {
	meterProvider := noopmeter.NewMeterProvider()
	tracerProvider := nooptracer.NewTracerProvider()
	tr, err := NewTracker(meterProvider, tracerProvider)
	require.NoError(t, err)

	ctx := context.Background()
	ctx, trk := tr.Start(ctx,
		WithAttributes(
			attribute.String("hello", "world"),
		),
	)
	err = trk.OnProfiles(ctx, []ch.ProfileEvent{
		{
			Name:  EventMemoryTrackerPeakUsage,
			Value: 10_000,
		},
		{
			Name:  EventMemoryTrackerUsage,
			Value: 5_000,
		},
		{
			Name:  EventOSCPUVirtualTimeMicroseconds,
			Value: 1_000_000,
		},
		{
			Name:  EventUserTimeMicroseconds,
			Value: 2_000_000,
		},
		{
			Name:  SystemTimeMicroseconds,
			Value: 500_000,
		},
		{
			Name:  InterfaceNativeSendBytes,
			Value: 1024,
		},
		{
			Name:  ReadCompressedBytes,
			Value: 2048,
		},
		{
			Name:  CompressedReadBufferBytes,
			Value: 4096,
		},
		{
			Name:  SelectedRows,
			Value: 100,
		},
		{
			Name:  OSReadBytes,
			Value: 8192,
		},
		{
			Name:  FileOpen,
			Value: 5,
		},
		{
			Name:  NetworkSendBytes,
			Value: 512,
		},
	})
	require.NoError(t, err)

	tkrPrivate := trk.(*track)
	require.Equal(t, int64(10_000), tkrPrivate.memoryTrackerPeakUsage)
	require.Equal(t, int64(5_000), tkrPrivate.memoryTrackerUsage)
	require.Equal(t, int64(1_000_000), tkrPrivate.virtualTime)
	require.Equal(t, int64(2_000_000), tkrPrivate.userTime)
	require.Equal(t, int64(500_000), tkrPrivate.systemTime)
	require.Equal(t, int64(5), tkrPrivate.fileOpen)
	require.Equal(t, int64(1024), tkrPrivate.interfaceNativeSendBytes)
	require.Equal(t, int64(2048), tkrPrivate.readCompressedBytes)
	require.Equal(t, int64(4096), tkrPrivate.compressedReadBufferBytes)
	require.Equal(t, int64(100), tkrPrivate.selectedRows)
	require.Equal(t, int64(8192), tkrPrivate.osReadBytes)
	require.Equal(t, int64(512), tkrPrivate.networkSendBytes)

	trPrivate := tr.(*tracker)
	require.Equal(t, int64(10_000), trPrivate.memoryTrackerPeakUsage())
	require.Equal(t, int64(5), trPrivate.fileOpen())

	trk.End()

	require.Equal(t, int64(0), trPrivate.memoryTrackerUsage())
	require.Equal(t, int64(0), trPrivate.fileOpen())
}

func TestTracker_MultipleTracks(t *testing.T) {
	meterProvider := noopmeter.NewMeterProvider()
	tracerProvider := nooptracer.NewTracerProvider()
	tr, err := NewTracker(meterProvider, tracerProvider)
	require.NoError(t, err)

	ctx := context.Background()

	// Create first track.
	ctx1, trk1 := tr.Start(ctx,
		WithAttributes(
			attribute.String("query", "query1"),
		),
	)
	err = trk1.OnProfiles(ctx1, []ch.ProfileEvent{
		{
			Name:  EventMemoryTrackerUsage,
			Value: 1000,
		},
		{
			Name:  EventMemoryTrackerPeakUsage,
			Value: 1500,
		},
		{
			Name:  FileOpen,
			Value: 3,
		},
	})
	require.NoError(t, err)

	// Create second track.
	ctx2, trk2 := tr.Start(ctx,
		WithAttributes(
			attribute.String("query", "query2"),
		),
	)
	err = trk2.OnProfiles(ctx2, []ch.ProfileEvent{
		{
			Name:  EventMemoryTrackerUsage,
			Value: 2000,
		},
		{
			Name:  EventMemoryTrackerPeakUsage,
			Value: 2500,
		},
		{
			Name:  FileOpen,
			Value: 7,
		},
	})
	require.NoError(t, err)

	trPrivate := tr.(*tracker)

	// Total memory usage should be sum of both tracks.
	require.Equal(t, int64(3000), trPrivate.memoryTrackerUsage())
	require.Equal(t, int64(4000), trPrivate.memoryTrackerPeakUsage())
	require.Equal(t, int64(10), trPrivate.fileOpen())

	// End first track.
	trk1.End()

	// After ending first track, only second track's values remain.
	require.Equal(t, int64(2000), trPrivate.memoryTrackerUsage())
	require.Equal(t, int64(2500), trPrivate.memoryTrackerPeakUsage())
	require.Equal(t, int64(7), trPrivate.fileOpen())

	// End second track.
	trk2.End()

	// After ending all tracks, values should be zero.
	require.Equal(t, int64(0), trPrivate.memoryTrackerUsage())
	require.Equal(t, int64(0), trPrivate.memoryTrackerPeakUsage())
	require.Equal(t, int64(0), trPrivate.fileOpen())
}

func TestTracker_IncrementalCounters(t *testing.T) {
	meterProvider := noopmeter.NewMeterProvider()
	tracerProvider := nooptracer.NewTracerProvider()
	tr, err := NewTracker(meterProvider, tracerProvider)
	require.NoError(t, err)

	ctx := context.Background()
	ctx, trk := tr.Start(ctx,
		WithAttributes(
			attribute.String("test", "incremental"),
		),
	)

	// First batch of events.
	err = trk.OnProfiles(ctx, []ch.ProfileEvent{
		{
			Name:  EventOSCPUVirtualTimeMicroseconds,
			Value: 1000,
		},
		{
			Name:  EventUserTimeMicroseconds,
			Value: 500,
		},
		{
			Name:  SystemTimeMicroseconds,
			Value: 250,
		},
		{
			Name:  InterfaceNativeSendBytes,
			Value: 100,
		},
		{
			Name:  ReadCompressedBytes,
			Value: 200,
		},
	})
	require.NoError(t, err)

	tkrPrivate := trk.(*track)
	require.Equal(t, int64(1000), tkrPrivate.virtualTime)
	require.Equal(t, int64(500), tkrPrivate.userTime)
	require.Equal(t, int64(250), tkrPrivate.systemTime)
	require.Equal(t, int64(100), tkrPrivate.interfaceNativeSendBytes)
	require.Equal(t, int64(200), tkrPrivate.readCompressedBytes)

	// Second batch of events - should increment.
	err = trk.OnProfiles(ctx, []ch.ProfileEvent{
		{
			Name:  EventOSCPUVirtualTimeMicroseconds,
			Value: 2000,
		},
		{
			Name:  EventUserTimeMicroseconds,
			Value: 1500,
		},
		{
			Name:  SystemTimeMicroseconds,
			Value: 750,
		},
		{
			Name:  InterfaceNativeSendBytes,
			Value: 300,
		},
		{
			Name:  ReadCompressedBytes,
			Value: 400,
		},
	})
	require.NoError(t, err)

	require.Equal(t, int64(3000), tkrPrivate.virtualTime)
	require.Equal(t, int64(2000), tkrPrivate.userTime)
	require.Equal(t, int64(1000), tkrPrivate.systemTime)
	require.Equal(t, int64(400), tkrPrivate.interfaceNativeSendBytes)
	require.Equal(t, int64(600), tkrPrivate.readCompressedBytes)

	trk.End()
}

func TestTracker_UnknownEvent(t *testing.T) {
	meterProvider := noopmeter.NewMeterProvider()
	tracerProvider := nooptracer.NewTracerProvider()
	tr, err := NewTracker(meterProvider, tracerProvider)
	require.NoError(t, err)

	ctx := context.Background()
	ctx, trk := tr.Start(ctx)

	// Unknown event should be ignored without error.
	err = trk.OnProfiles(ctx, []ch.ProfileEvent{
		{
			Name:  "UnknownEvent",
			Value: 12345,
		},
	})
	require.NoError(t, err)

	trk.End()
}
