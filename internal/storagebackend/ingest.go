package storagebackend

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/otlp/pdataconv"
	siglog "github.com/oteldb/storage/signal/log"
	sigprofile "github.com/oteldb/storage/signal/profile"
	sigtrace "github.com/oteldb/storage/signal/trace"
)

// ConsumeTraces ingests an OTLP traces batch into the storage engine. It is the traces ingestion
// sink used when the storage backend serves traces.
func (b *Backend) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	var batch sigtrace.Traces
	pdataconv.AppendTraces(&batch, td)

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

	if _, err := b.store.WriteProfiles(ctx, batch); err != nil {
		return errors.Wrap(err, "write profiles")
	}
	return nil
}
