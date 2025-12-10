package chstorage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

type tracesRestore struct {
	client ClickHouseClient
	tables Tables
	logger *zap.Logger
}

func (r *tracesRestore) Do(ctx context.Context, root string) error {
	dirs, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		step := filepath.Join(root, d.Name())
		if err := r.restore(ctx, step); err != nil {
			return errors.Wrapf(err, "restore dir %q", step)
		}
	}
	return nil
}

func (r *tracesRestore) restore(ctx context.Context, dir string) error {
	stopwatch := time.Now()
	r.logger.Info("Restoring traces dir", zap.String("dir", dir))

	if err := r.restoreSpans(ctx, dir); err != nil {
		return errors.Wrap(err, "restore spans")
	}

	r.logger.Info("Restored traces dir", zap.Duration("took", time.Since(stopwatch)), zap.String("dir", dir))
	return nil
}

func (r *tracesRestore) restoreSpans(ctx context.Context, dir string) error {
	w, err := openBackupReader(dir, "traces")
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()

	var (
		traceID    proto.ColRawOf[otelstorage.TraceID]
		spanID     proto.ColRawOf[otelstorage.SpanID]
		parentSpan proto.ColRawOf[otelstorage.SpanID]
		traceState proto.ColStr

		name          = new(proto.ColStr).LowCardinality()
		kind          proto.ColEnum8
		startTime     = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)
		endTime       = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)
		statusCode    proto.ColUInt8
		statusMessage = new(proto.ColStr).LowCardinality()
		batchID       proto.ColUUID
		scopeName     = new(proto.ColStr).LowCardinality()
		scopeVersion  = new(proto.ColStr).LowCardinality()

		eventsNames      = new(proto.ColStr).Array()
		eventsTimestamps = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano).Array()
		eventsAttributes = new(proto.ColBytes).Array()

		linksTraceIDs    = proto.NewArray(&proto.ColRawOf[otelstorage.TraceID]{})
		linksSpanIDs     = proto.NewArray(&proto.ColRawOf[otelstorage.SpanID]{})
		linksTracestates = new(proto.ColStr).Array()
		linksAttributes  = new(proto.ColBytes).Array()

		attributes = NewAttributes(colAttrs)
		scope      = NewAttributes(colScope)
		resource   = NewAttributes(colResource)

		columns = MergeColumns(
			Columns{
				{Name: "trace_id", Data: &traceID},
				{Name: "span_id", Data: &spanID},
				{Name: "trace_state", Data: &traceState},
				{Name: "parent_span_id", Data: &parentSpan},

				{Name: "name", Data: name},
				{Name: "kind", Data: &kind},
				{Name: "start", Data: startTime},
				{Name: "end", Data: endTime},
				{Name: "status_code", Data: &statusCode},
				{Name: "status_message", Data: statusMessage},
				{Name: "batch_id", Data: &batchID},
			},
			Columns{
				{Name: "events_timestamps", Data: eventsTimestamps},
				{Name: "events_names", Data: eventsNames},
				{Name: "events_attributes", Data: eventsAttributes},

				{Name: "links_trace_ids", Data: linksTraceIDs},
				{Name: "links_span_ids", Data: linksSpanIDs},
				{Name: "links_tracestates", Data: linksTracestates},
				{Name: "links_attributes", Data: linksAttributes},
			},
			attributes.Columns(),
			scope.Columns(),
			resource.Columns(),
		)

		block proto.Block
		rd    = proto.NewReader(w)
	)

	for {
		columns.Reset()
		if err := block.DecodeRawBlock(rd, 54451, columns.Result()); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			if err != nil {
				return err
			}
			break
		}

		var (
			sc   = newSpanColumns()
			tags = newSpanAttrsColumns()
		)
		for i := 0; i < startTime.Rows(); i++ {
			var s tracestorage.Span
			s.TraceID = traceID.Row(i)
			s.SpanID = spanID.Row(i)
			s.TraceState = traceState.Row(i)
			s.ParentSpanID = parentSpan.Row(i)

			s.Name = name.Row(i)
			s.Kind = int32(kind.Row(i))
			s.Start = otelstorage.NewTimestampFromTime(startTime.Row(i))
			s.End = otelstorage.NewTimestampFromTime(endTime.Row(i))
			s.StatusCode = int32(statusCode.Row(i))
			s.StatusMessage = statusMessage.Row(i)
			s.BatchID = batchID.Row(i)

			if len(eventsTimestamps.Row(i)) > 0 {
				s.Events = r.convertEvents(
					eventsTimestamps.Row(i),
					eventsNames.Row(i),
					eventsAttributes.Row(i),
				)
			}

			if len(linksTraceIDs.Row(i)) > 0 {
				s.Links = r.convertLinks(
					linksTraceIDs.Row(i),
					linksSpanIDs.Row(i),
					linksTracestates.Row(i),
					linksAttributes.Row(i),
				)
			}

			s.Attrs = attributes.Row(i)
			s.ResourceAttrs = resource.Row(i)
			s.ScopeName = scopeName.Row(i)
			s.ScopeVersion = scopeVersion.Row(i)
			s.ScopeAttrs = scope.Row(i)

			sc.AddRow(s)
			tags.AddAttrs(traceql.ScopeSpan, s.Attrs)
			tags.AddAttrs(traceql.ScopeResource, s.ResourceAttrs)
			tags.AddAttrs(traceql.ScopeInstrumentation, s.ScopeAttrs)
		}

		grp, grpCtx := errgroup.WithContext(ctx)
		grp.Go(func() error {
			ctx := grpCtx

			input := tags.Input()
			if err := r.client.Do(ctx, ch.Query{
				Body:  input.Into(r.tables.Tags),
				Input: input,
			}); err != nil {
				return errors.Wrap(err, "insert trace tags")
			}
			return nil
		})
		grp.Go(func() error {
			ctx := grpCtx

			input := sc.Input()
			if err := r.client.Do(ctx, ch.Query{
				Body:  input.Into(r.tables.Spans),
				Input: input,
			}); err != nil {
				return errors.Wrap(err, "insert spans")
			}
			return nil
		})
		if err := grp.Wait(); err != nil {
			return err
		}
	}
	return nil
}

func (r *tracesRestore) convertEvents(
	eventTimestamps []time.Time,
	eventNames []string,
	eventAttrs [][]byte,
) []tracestorage.Event {
	events := make([]tracestorage.Event, len(eventTimestamps))
	for i := range eventTimestamps {
		attrs, err := decodeAttributes(eventAttrs[i])
		if err != nil {
			continue
		}
		events[i].Timestamp = otelstorage.NewTimestampFromTime(eventTimestamps[i])
		events[i].Name = eventNames[i]
		events[i].Attrs = attrs
	}
	return events
}

func (r *tracesRestore) convertLinks(
	linksTraceIDs []otelstorage.TraceID,
	linksSpanIDs []otelstorage.SpanID,
	linksTracestates []string,
	linksAttributes [][]byte,
) []tracestorage.Link {
	links := make([]tracestorage.Link, len(linksTraceIDs))
	for i := range linksTraceIDs {
		attrs, err := decodeAttributes(linksAttributes[i])
		if err != nil {
			continue
		}
		links[i].TraceID = linksTraceIDs[i]
		links[i].SpanID = linksSpanIDs[i]
		links[i].TraceState = linksTracestates[i]
		links[i].Attrs = attrs
	}
	return links
}
