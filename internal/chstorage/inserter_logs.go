package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/globalmetric"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/semconv"
	"github.com/go-faster/oteldb/internal/xsync"
)

type recordWriter struct {
	logs     *logColumns
	attrs    *logAttrMapColumns
	inserter *Inserter
}

var _ logstorage.RecordWriter = (*recordWriter)(nil)

// Add adds record to the batch.
func (w *recordWriter) Add(record logstorage.Record) error {
	w.logs.AddRow(record)
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
				metric.WithAttributes(
					semconv.Signal(semconv.SignalLogs),
				),
			)
		}
		span.End()
	}()

	lg := zctx.From(ctx).Named("ch").WithOptions(zap.IncreaseLevel(i.chLogLevel))
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx

		table := i.tables.Logs
		ctx, track := i.tracker.Start(ctx, globalmetric.WithAttributes(
			semconv.Signal(semconv.SignalLogs),
			attribute.String("chstorage.table", table),
		))
		defer track.End()

		if err := i.ch.Do(ctx, ch.Query{
			Logger:          lg,
			Body:            logs.Body(table),
			Input:           logs.Input(),
			OnProfileEvents: track.OnProfiles,
		}); err != nil {
			return errors.Wrap(err, "insert records")
		}
		i.stats.BatchSize.Record(ctx, int64(logs.body.Rows()), metric.WithAttributes(
			semconv.Signal(semconv.SignalLogs),
			attribute.String("chstorage.table", table),
		))
		i.stats.InsertedRecords.Add(ctx, int64(logs.body.Rows()))

		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx

		table := i.tables.LogAttrs
		ctx, track := i.tracker.Start(ctx, globalmetric.WithAttributes(
			semconv.Signal(semconv.SignalLogs),
			attribute.String("chstorage.table", table),
		))
		defer track.End()

		if err := i.ch.Do(ctx, ch.Query{
			Logger:          lg,
			Body:            attrs.Body(table),
			Input:           attrs.Input(),
			OnProfileEvents: track.OnProfiles,
		}); err != nil {
			return errors.Wrap(err, "insert labels")
		}
		i.stats.BatchSize.Record(ctx, int64(attrs.name.Rows()), metric.WithAttributes(
			semconv.Signal(semconv.SignalLogs),
			attribute.String("chstorage.table", table),
		))
		i.stats.InsertedLogLabels.Add(ctx, int64(attrs.name.Rows()), metric.WithAttributes())

		return nil
	})
	return grp.Wait()
}
