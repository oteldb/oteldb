package chstorage

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/globalmetric"
	"github.com/go-faster/oteldb/internal/semconv"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/tracestorage"
	"github.com/go-faster/oteldb/internal/xsync"
)

type spanWriter struct {
	spans *spanColumns
	attrs *spanAttrsColumns

	inserter *Inserter
}

var _ tracestorage.SpanWriter = (*spanWriter)(nil)

// Add adds record to the batch.
func (w *spanWriter) Add(s tracestorage.Span) error {
	w.spans.AddRow(s)
	w.attrs.AddAttrs(traceql.ScopeSpan, s.Attrs)
	w.attrs.AddAttrs(traceql.ScopeResource, s.ResourceAttrs)
	w.attrs.AddAttrs(traceql.ScopeInstrumentation, s.ScopeAttrs)
	return nil
}

// Submit sends batch.
func (w *spanWriter) Submit(ctx context.Context) error {
	return w.inserter.submitTraces(ctx, w.spans, w.attrs)
}

// Close frees resources.
func (w *spanWriter) Close() error {
	spanColumnsPool.Put(w.spans)
	spanAttrsColumnsPool.Put(w.attrs)
	return nil
}

var _ tracestorage.Inserter = (*Inserter)(nil)

// SpanWriter returns a new [tracestorage.SpanWriter]
func (i *Inserter) SpanWriter(ctx context.Context) (tracestorage.SpanWriter, error) {
	return &spanWriter{
		spans:    xsync.GetReset(spanColumnsPool),
		attrs:    xsync.GetReset(spanAttrsColumnsPool),
		inserter: i,
	}, nil
}

// submitTraces inserts given traces.
func (i *Inserter) submitTraces(
	ctx context.Context,
	spans *spanColumns,
	attrs *spanAttrsColumns,
) (rerr error) {
	ctx, span := i.tracer.Start(ctx, "chstorage.traces.submitTraces", trace.WithAttributes(
		attribute.Int("chstorage.spans_count", spans.spanID.Rows()),
	))

	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else {
			i.stats.Inserts.Add(ctx, 1,
				metric.WithAttributes(semconv.Signal(semconv.SignalTraces)),
			)
		}
		span.End()
	}()

	ctx, track := i.tracker.Start(ctx, globalmetric.WithAttributes(
		semconv.Signal(semconv.SignalTraces),
	))
	defer track.End()

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx

		table := i.tables.Spans
		if err := i.do(ctx, semconv.SignalTraces, table, spans.Body(table), spans.Input()); err != nil {
			return errors.Wrap(err, "insert spans")
		}
		i.stats.InsertedSpans.Add(ctx, int64(spans.spanID.Rows()))

		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx

		table := i.tables.Tags
		if err := i.do(ctx, semconv.SignalTraces, table, attrs.Body(table), attrs.Input()); err != nil {
			return errors.Wrap(err, "insert tags")
		}
		i.stats.InsertedTags.Add(ctx, int64(attrs.name.Rows()))

		return nil
	})
	return grp.Wait()
}
