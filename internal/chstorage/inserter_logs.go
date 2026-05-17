package chstorage

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/semconv"
	"github.com/oteldb/oteldb/internal/xsync"
)

type recordWriter struct {
	logs        *logColumns
	attrs       *logAttrMapColumns
	inserter    *Inserter
	droppedRows int             // count of rows dropped due to tenant validation
	ctx         context.Context // context for write validation
}

var _ logstorage.RecordWriter = (*recordWriter)(nil)

// Add adds record to the batch.
func (w *recordWriter) Add(record logstorage.Record) error {
	// Resolve tenant_id from resource attributes if mapper is configured
	var tenantID string
	if m := w.inserter.tenantMapper; m != nil {
		var ok bool
		tenantID, ok = m.Resolve(record.ResourceAttrs)
		if !ok {
			// No tenant matched and no default → drop the record
			w.droppedRows++
			return nil
		}

		// Validate tenant_id against write Decision if validator is configured
		if wv := w.inserter.writeValidator; wv != nil {
			if !wv.IsAuthorized(w.ctx, tenantID) {
				// Unauthorized write → drop the record
				w.droppedRows++
				return nil
			}
		}
	}

	w.logs.AddRow(record, tenantID)
	w.attrs.AddAttrs(record.Attrs)
	w.attrs.AddAttrs(record.ResourceAttrs)
	w.attrs.AddAttrs(record.ScopeAttrs)
	return nil
}

// Submit sends batch.
func (w *recordWriter) Submit(ctx context.Context) error {
	return w.inserter.submitLogs(ctx, w.logs, w.attrs)
}

// Close frees resources.
func (w *recordWriter) Close() error {
	logColumnsPool.Put(w.logs)
	logAttrMapColumnsPool.Put(w.attrs)
	return nil
}

var _ logstorage.Inserter = (*Inserter)(nil)

// RecordWriter returns a new [logstorage.RecordWriter]
func (i *Inserter) RecordWriter(ctx context.Context) (logstorage.RecordWriter, error) {
	logs := xsync.GetReset(logColumnsPool)
	attrs := xsync.GetReset(logAttrMapColumnsPool)

	return &recordWriter{
		logs:     logs,
		attrs:    attrs,
		inserter: i,
		ctx:      ctx,
	}, nil
}

func (i *Inserter) submitLogs(ctx context.Context, logs *logColumns, attrs *logAttrMapColumns) (rerr error) {
	ctx, span := i.tracer.Start(ctx, "chstorage.logs.submitLogs", trace.WithAttributes(
		attribute.Int("chstorage.records_count", logs.body.Rows()),
		attribute.Int("chstorage.attrs_count", attrs.name.Rows()),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else {
			i.stats.Inserts.Add(ctx, 1,
				metric.WithAttributes(semconv.Signal(semconv.SignalLogs)),
			)
		}
		span.End()
	}()

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx

		table := i.tables.Logs
		if err := i.do(ctx, semconv.SignalLogs, table, logs.Body(table), logs.Input()); err != nil {
			return errors.Wrap(err, "insert records")
		}
		i.stats.InsertedRecords.Add(ctx, int64(logs.body.Rows()))

		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx

		table := i.tables.LogAttrs
		if err := i.do(ctx, semconv.SignalLogs, table, attrs.Body(table), attrs.Input()); err != nil {
			return errors.Wrap(err, "insert log labels")
		}
		i.stats.InsertedLogLabels.Add(ctx, int64(attrs.name.Rows()))

		return nil
	})
	return grp.Wait()
}
