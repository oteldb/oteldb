package ch2storagebackend

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/metricstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// destination receives migrated data.
//
// It is not a pdata interface: the embedded engine takes the internal signal model directly, and
// routing every row through pdata to satisfy a shared signature would undo that. Each destination
// converts from the source model in whatever way suits it.
type destination interface {
	WriteLogs(ctx context.Context, records []logstorage.Record) error
	WriteTraces(ctx context.Context, spans []tracestorage.Span) error
	WriteNumberPoints(ctx context.Context, points []metricstorage.NumberPoint) error
	WriteMetrics(ctx context.Context, md pmetric.Metrics) error
	Close() error
}

// backendDest writes into an embedded [storagebackend.Backend], bypassing pdata for everything the
// engine can take in its own model.
type backendDest struct {
	back  *storagebackend.Backend
	attrs *attrConv
}

func (d *backendDest) WriteLogs(ctx context.Context, records []logstorage.Record) error {
	if err := d.back.WriteLogs(ctx, ConvertLogs(records, d.attrs)); err != nil {
		return errors.Wrap(err, "write logs")
	}

	return nil
}

func (d *backendDest) WriteTraces(ctx context.Context, spans []tracestorage.Span) error {
	if err := d.back.WriteTraces(ctx, ConvertTraces(spans, d.attrs)); err != nil {
		return errors.Wrap(err, "write traces")
	}

	return nil
}

func (d *backendDest) WriteNumberPoints(ctx context.Context, points []metricstorage.NumberPoint) error {
	if err := d.back.WriteMetrics(ctx, ConvertNumberPoints(points, d.attrs)); err != nil {
		return errors.Wrap(err, "write metrics")
	}

	return nil
}

func (d *backendDest) WriteMetrics(ctx context.Context, md pmetric.Metrics) error {
	if err := d.back.ConsumeMetrics(ctx, md); err != nil {
		return errors.Wrap(err, "consume metrics")
	}

	return nil
}

func (d *backendDest) Close() error { return nil }
