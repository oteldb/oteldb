package globalmetric

import (
	"context"
	"sync"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// tracker tracks global metrics from multiple tracks.
type tracker struct {
	mu     sync.Mutex
	tracks []*track

	tracer trace.Tracer

	virtualTime               metric.Int64Counter
	userTime                  metric.Int64Counter
	systemTime                metric.Int64Counter
	interfaceNativeSendBytes  metric.Int64Counter
	readCompressedBytes       metric.Int64Counter
	compressedReadBufferBytes metric.Int64Counter
	selectedRows              metric.Int64Counter
	osReadBytes               metric.Int64Counter
	networkSendBytes          metric.Int64Counter
}

func (t *tracker) memoryTrackerUsage() int64 {
	var total int64
	for _, c := range t.tracks {
		c.mu.Lock()
		total += c.memoryTrackerUsage
		c.mu.Unlock()
	}

	return total
}

func (t *tracker) memoryTrackerPeakUsage() int64 {
	var total int64
	for _, c := range t.tracks {
		c.mu.Lock()
		total += c.memoryTrackerPeakUsage
		c.mu.Unlock()
	}

	return total
}

func (t *tracker) fileOpen() int64 {
	var total int64
	for _, c := range t.tracks {
		c.mu.Lock()
		total += c.fileOpen
		c.mu.Unlock()
	}

	return total
}

func (t *tracker) remove(ctx *track) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, c := range t.tracks {
		if c == ctx {
			t.tracks = append(t.tracks[:i], t.tracks[i+1:]...)
			return
		}
	}
}

// TrackOption configures a [track].
type TrackOption func(t *track)

// WithAttributes sets attributes for the track.
func WithAttributes(attrs ...attribute.KeyValue) TrackOption {
	return func(t *track) {
		t.attributes = append(t.attributes, attrs...)
	}
}

// Start creates a new [track] with given options.
func (t *tracker) Start(ctx context.Context, options ...TrackOption) (context.Context, Track) {
	trk := &track{
		tracker: t,
	}
	for _, o := range options {
		o(trk)
	}

	ctx, span := t.tracer.Start(ctx, "Tracker.Start",
		trace.WithAttributes(trk.attributes...),
	)
	trk.span = span

	t.tracks = append(t.tracks, trk)

	return ctx, trk
}

// track represents a tracked ClickHouse query or series of queries.
type track struct {
	attributes []attribute.KeyValue
	tracker    *tracker
	span       trace.Span

	mu                        sync.Mutex
	memoryTrackerPeakUsage    int64
	memoryTrackerUsage        int64
	virtualTime               int64
	userTime                  int64
	systemTime                int64
	fileOpen                  int64
	interfaceNativeSendBytes  int64
	readCompressedBytes       int64
	compressedReadBufferBytes int64
	selectedRows              int64
	osReadBytes               int64
	networkSendBytes          int64
}

func (t *track) increaseVirtualTime(ctx context.Context, delta int64) {
	t.virtualTime += delta
	t.tracker.virtualTime.Add(ctx, delta, metric.WithAttributes(t.attributes...))
}

func (t *track) increaseUserTime(ctx context.Context, value int64) {
	t.userTime += value
	t.tracker.userTime.Add(ctx, value, metric.WithAttributes(t.attributes...))
}

func (t *track) increaseSystemTime(ctx context.Context, value int64) {
	t.systemTime += value
	t.tracker.systemTime.Add(ctx, value, metric.WithAttributes(t.attributes...))
}

func (t *track) increaseInterfaceNativeSendBytes(ctx context.Context, value int64) {
	t.interfaceNativeSendBytes += value
	t.tracker.interfaceNativeSendBytes.Add(ctx, value, metric.WithAttributes(t.attributes...))
}

func (t *track) increaseReadCompressedBytes(ctx context.Context, value int64) {
	t.readCompressedBytes += value
	t.tracker.readCompressedBytes.Add(ctx, value, metric.WithAttributes(t.attributes...))
}

func (t *track) increaseCompressedReadBufferBytes(ctx context.Context, value int64) {
	t.compressedReadBufferBytes += value
	t.tracker.compressedReadBufferBytes.Add(ctx, value, metric.WithAttributes(t.attributes...))
}

func (t *track) increaseSelectedRows(ctx context.Context, value int64) {
	t.selectedRows += value
	t.tracker.selectedRows.Add(ctx, value, metric.WithAttributes(t.attributes...))
}

func (t *track) increaseOSReadBytes(ctx context.Context, value int64) {
	t.osReadBytes += value
	t.tracker.osReadBytes.Add(ctx, value, metric.WithAttributes(t.attributes...))
}

func (t *track) increaseNetworkSendBytes(ctx context.Context, value int64) {
	t.networkSendBytes += value
	t.tracker.networkSendBytes.Add(ctx, value, metric.WithAttributes(t.attributes...))
}

// Selected profile events.
//
// See https://github.com/ClickHouse/ClickHouse/blob/99a58256d5ce16d7d67f20c088e9c12f6a11db2c/src/Common/ProfileEvents.cpp#L539
const (
	EventMemoryTrackerUsage           = "MemoryTrackerUsage"
	EventMemoryTrackerPeakUsage       = "MemoryTrackerPeakUsage"
	EventOSCPUVirtualTimeMicroseconds = "OSCPUVirtualTimeMicroseconds"
	EventUserTimeMicroseconds         = "UserTimeMicroseconds"
	SystemTimeMicroseconds            = "SystemTimeMicroseconds"
	InterfaceNativeSendBytes          = "InterfaceNativeSendBytes"
	ReadCompressedBytes               = "ReadCompressedBytes"
	CompressedReadBufferBytes         = "CompressedReadBufferBytes"
	SelectedRows                      = "SelectedRows"
	OSReadBytes                       = "OSReadBytes"
	FileOpen                          = "FileOpen"
	NetworkSendBytes                  = "NetworkSendBytes"
)

func (t *track) OnProfiles(ctx context.Context, events []ch.ProfileEvent) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, e := range events {
		switch e.Name {
		case EventMemoryTrackerUsage:
			t.memoryTrackerUsage = e.Value
		case EventMemoryTrackerPeakUsage:
			t.memoryTrackerPeakUsage = e.Value
		case EventOSCPUVirtualTimeMicroseconds:
			// CPU time spent seen by OS. Does not include involuntary waits due to virtualization.
			t.increaseVirtualTime(ctx, e.Value)
		case EventUserTimeMicroseconds:
			// Total time spent in processing (queries and other tasks) threads executing CPU instructions in user mode.
			t.increaseUserTime(ctx, e.Value)
		case SystemTimeMicroseconds:
			// Total time spent in processing (queries and other tasks) threads executing CPU instructions in OS kernel mode.
			t.increaseSystemTime(ctx, e.Value)
		case InterfaceNativeSendBytes:
			// Number of bytes sent from server to client.
			t.increaseInterfaceNativeSendBytes(ctx, e.Value)
		case ReadCompressedBytes:
			// Number of compressed bytes read.
			t.increaseReadCompressedBytes(ctx, e.Value)
		case CompressedReadBufferBytes:
			// Number of bytes read from compressed sources.
			t.increaseCompressedReadBufferBytes(ctx, e.Value)
		case SelectedRows:
			// Number of rows selected from tables.
			t.increaseSelectedRows(ctx, e.Value)
		case OSReadBytes:
			// Number of bytes read from disk.
			t.increaseOSReadBytes(ctx, e.Value)
		case FileOpen:
			// Number of files opened.
			t.fileOpen = e.Value
		case NetworkSendBytes:
			// Number of bytes sent over network.
			t.increaseNetworkSendBytes(ctx, e.Value)
		default:
			continue
		}
	}

	return nil
}

func (t *track) End() {
	t.span.AddEvent("End", trace.WithAttributes(
		attribute.Int64(EventMemoryTrackerUsage, t.memoryTrackerUsage),
		attribute.Int64(EventMemoryTrackerPeakUsage, t.memoryTrackerPeakUsage),
		attribute.Int64(EventOSCPUVirtualTimeMicroseconds, t.virtualTime),
		attribute.Int64(EventUserTimeMicroseconds, t.userTime),
		attribute.Int64(SystemTimeMicroseconds, t.systemTime),
		attribute.Int64(InterfaceNativeSendBytes, t.interfaceNativeSendBytes),
		attribute.Int64(ReadCompressedBytes, t.readCompressedBytes),
		attribute.Int64(CompressedReadBufferBytes, t.compressedReadBufferBytes),
		attribute.Int64(SelectedRows, t.selectedRows),
		attribute.Int64(OSReadBytes, t.osReadBytes),
		attribute.Int64(FileOpen, t.fileOpen),
		attribute.Int64(NetworkSendBytes, t.networkSendBytes),
	))
	t.tracker.remove(t)
	t.span.End()
}

type Track interface {
	End()
	OnProfiles(ctx context.Context, events []ch.ProfileEvent) error
}

type Tracker interface {
	Start(ctx context.Context, opts ...TrackOption) (context.Context, Track)
}

func NewTracker(meterProvider metric.MeterProvider, tracerProvider trace.TracerProvider) (Tracker, error) {
	meter := meterProvider.Meter("github.com/oteldb/oteldb/internal/globalmetric",
		metric.WithSchemaURL("https://schema.oteldb.tech/metrics/v1"),
		metric.WithInstrumentationVersion("0.1.0"),
	)
	tracer := tracerProvider.Tracer("github.com/oteldb/oteldb/internal/globalmetric",
		trace.WithSchemaURL("https://schema.oteldb.tech/trace/v1"),
		trace.WithInstrumentationVersion("0.1.0"),
	)
	virtualTime, err := meter.Int64Counter("clickhouse.cpu.virtual_time_microseconds",
		metric.WithDescription("Total virtual CPU time used by ClickHouse in microseconds"),
		metric.WithUnit("us"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "virtual time counter")
	}
	userTime, err := meter.Int64Counter("clickhouse.cpu.user_time_microseconds",
		metric.WithDescription("Total user CPU time used by ClickHouse in microseconds"),
		metric.WithUnit("us"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "user time counter")
	}
	systemTime, err := meter.Int64Counter("clickhouse.cpu.system_time_microseconds",
		metric.WithDescription("Total system CPU time used by ClickHouse in microseconds"),
		metric.WithUnit("us"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "system time counter")
	}
	interfaceNativeSendBytes, err := meter.Int64Counter("clickhouse.interface.native_send_bytes",
		metric.WithDescription("Number of bytes sent from server to client"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "interface native send bytes counter")
	}
	readCompressedBytes, err := meter.Int64Counter("clickhouse.read.compressed_bytes",
		metric.WithDescription("Number of compressed bytes read"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "read compressed bytes counter")
	}
	compressedReadBufferBytes, err := meter.Int64Counter("clickhouse.read.compressed_buffer_bytes",
		metric.WithDescription("Number of bytes read from compressed sources"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "compressed read buffer bytes counter")
	}
	selectedRows, err := meter.Int64Counter("clickhouse.selected_rows",
		metric.WithDescription("Number of rows selected from tables"),
		metric.WithUnit("{rows}"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "selected rows counter")
	}
	osReadBytes, err := meter.Int64Counter("clickhouse.os.read_bytes",
		metric.WithDescription("Number of bytes read from disk"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "os read bytes counter")
	}
	networkSendBytes, err := meter.Int64Counter("clickhouse.network.send_bytes",
		metric.WithDescription("Number of bytes sent over network"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "network send bytes counter")
	}
	memoryTrackerUsage, err := meter.Int64ObservableGauge("clickhouse.memory_tracker.usage",
		metric.WithDescription("Current memory tracker usage in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "memory tracker usage gauge")
	}
	memoryTrackerPeakUsage, err := meter.Int64ObservableGauge("clickhouse.memory_tracker.peak_usage",
		metric.WithDescription("Peak memory tracker usage in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "memory tracker peak usage gauge")
	}
	fileOpen, err := meter.Int64ObservableGauge("clickhouse.file.open",
		metric.WithDescription("Number of files opened"),
		metric.WithUnit("{files}"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "file open gauge")
	}

	t := &tracker{
		tracer: tracer,

		virtualTime:               virtualTime,
		userTime:                  userTime,
		systemTime:                systemTime,
		interfaceNativeSendBytes:  interfaceNativeSendBytes,
		readCompressedBytes:       readCompressedBytes,
		compressedReadBufferBytes: compressedReadBufferBytes,
		selectedRows:              selectedRows,
		osReadBytes:               osReadBytes,
		networkSendBytes:          networkSendBytes,
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		t.mu.Lock()
		defer t.mu.Unlock()
		observer.ObserveInt64(memoryTrackerUsage, t.memoryTrackerUsage())
		observer.ObserveInt64(memoryTrackerPeakUsage, t.memoryTrackerPeakUsage())
		observer.ObserveInt64(fileOpen, t.fileOpen())
		return nil
	},
		memoryTrackerUsage,
		memoryTrackerPeakUsage,
		fileOpen,
	)
	if err != nil {
		return nil, errors.Wrap(err, "register memory tracker callback")
	}

	return t, nil
}
