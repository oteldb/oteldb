package storagebackend

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/oteldb/storage/signal"
)

const (
	meterName = "github.com/oteldb/oteldb/internal/storagebackend"
	// droppedRecords counts what an OTLP batch carried that the conversion could not represent.
	droppedRecords = "oteldb.storage.dropped_records"

	// reasonNoTimestamp is a log record carrying neither time_unix_nano nor
	// observed_time_unix_nano: nothing to sort or query it by.
	reasonNoTimestamp = "no_timestamp"
)

const (
	droppedSignalKey = attribute.Key("signal")
	droppedReasonKey = attribute.Key("reason")
)

// WithMeterProvider reports ingest-side drops through mp.
//
// The Consume* sinks implement the collector's consumer signatures, which return only an error, so
// a batch that was mostly written has no in-band way to report the records it refused. This counter
// is the only place those become visible; without it they vanish silently. A Backend built without
// a provider still ingests, it just does not count.
func WithMeterProvider(mp otelmetric.MeterProvider) Option {
	return func(b *Backend) {
		if mp == nil {
			return
		}
		c, err := mp.Meter(meterName).Int64Counter(droppedRecords,
			otelmetric.WithDescription("Records an OTLP batch carried that could not be represented, by signal and reason."),
		)
		if err != nil {
			return
		}
		b.dropped = c
	}
}

func (b *Backend) countDropped(ctx context.Context, sig signal.Signal, reason string, n int) {
	if n <= 0 || b.dropped == nil {
		return
	}
	b.dropped.Add(ctx, int64(n), otelmetric.WithAttributes(
		droppedSignalKey.String(sig.String()),
		droppedReasonKey.String(reason),
	))
}
