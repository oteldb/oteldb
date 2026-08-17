package storagebackend

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/otlp/pdataconv"
	siglog "github.com/oteldb/storage/signal/log"
	sigmetric "github.com/oteldb/storage/signal/metric"
	sigprofile "github.com/oteldb/storage/signal/profile"
	sigtrace "github.com/oteldb/storage/signal/trace"
)

// ConsumeTraces ingests an OTLP traces batch into the storage engine. It is the traces ingestion
// sink used when the storage backend serves traces.
func (b *Backend) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	var batch sigtrace.Traces
	pdataconv.AppendTraces(&batch, td)

	if b.store == nil {
		return ErrNoEngine
	}
	if _, err := b.store.WriteTraces(ctx, batch); err != nil {
		return errors.Wrap(err, "write traces")
	}
	return nil
}

// ConsumeLogs ingests an OTLP logs batch into the storage engine. It is the logs ingestion sink
// used when the storage backend serves logs.
func (b *Backend) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	var batch siglog.Logs
	pdataconv.AppendLogs(&batch, ld)

	if b.store == nil {
		return ErrNoEngine
	}
	if _, err := b.store.WriteLogs(ctx, batch); err != nil {
		return errors.Wrap(err, "write logs")
	}
	return nil
}

// ConsumeProfiles ingests an OTLP profiles batch into the storage engine. It is the profiles
// ingestion sink used when the storage backend serves profiles.
func (b *Backend) ConsumeProfiles(ctx context.Context, pd pprofile.Profiles) error {
	var batch sigprofile.Profiles
	pdataconv.AppendProfiles(&batch, pd)

	if b.store == nil {
		return ErrNoEngine
	}
	if _, err := b.store.WriteProfiles(ctx, batch); err != nil {
		return errors.Wrap(err, "write profiles")
	}
	return nil
}

// The Write* methods below take the engine's native batch types directly, for producers that
// already build them (a bulk migration out of another store, for instance). The Consume* sinks
// above are the OTLP entry points and go through pdataconv; these skip that translation.

// WriteMetrics ingests a native metrics batch into the storage engine.
func (b *Backend) WriteMetrics(ctx context.Context, batch sigmetric.Metrics) error {
	if b.store == nil {
		return ErrNoEngine
	}
	if _, err := b.store.WriteMetrics(ctx, batch); err != nil {
		return errors.Wrap(err, "write metrics")
	}
	return nil
}

// WriteLogs ingests a native logs batch into the storage engine.
func (b *Backend) WriteLogs(ctx context.Context, batch siglog.Logs) error {
	if b.store == nil {
		return ErrNoEngine
	}
	if _, err := b.store.WriteLogs(ctx, batch); err != nil {
		return errors.Wrap(err, "write logs")
	}
	return nil
}

// WriteTraces ingests a native traces batch into the storage engine.
func (b *Backend) WriteTraces(ctx context.Context, batch sigtrace.Traces) error {
	if b.store == nil {
		return ErrNoEngine
	}
	if _, err := b.store.WriteTraces(ctx, batch); err != nil {
		return errors.Wrap(err, "write traces")
	}
	return nil
}
