package chstorage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

type tracesBackup struct {
	client ClickHouseClient
	tables Tables
	logger *zap.Logger
}

func (b *tracesBackup) Do(ctx context.Context, dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return errors.Wrap(err, "create directory")
	}

	mint, maxt, err := queryMinMaxTimestamp(ctx, b.client,
		[2]string{b.tables.Spans, "start"},
	)
	if err != nil {
		return errors.Wrap(err, "query min/max timestamp")
	}
	if mint.IsZero() && maxt.IsZero() {
		b.logger.Info("No traces to backup")
		return nil
	}

	step := 24 * time.Hour
	start := mint.Truncate(step)
	end := maxt.Truncate(step).Add(step)
	for ts := start; ts.Before(end); ts = ts.Add(step) {
		if err := b.backup(ctx, dir, ts, ts.Add(step)); err != nil {
			return err
		}
	}
	return nil
}

func (b *tracesBackup) backup(ctx context.Context, root string, start, end time.Time) error {
	var (
		stopwatch = time.Now()
		dir       = filepath.Join(root, start.Format("2006-01-02_15-04-05"))
	)
	b.logger.Info("Backing up traces", zap.Time("start", start))

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return errors.Wrap(err, "create directory")
	}

	if err := b.backupSpans(ctx, dir, start, end); err != nil {
		return errors.Wrap(err, "backup spans")
	}

	b.logger.Info("Backed up traces", zap.Duration("took", time.Since(stopwatch)), zap.String("dir", dir))
	return nil
}

func (b *tracesBackup) backupSpans(ctx context.Context, dir string, start, end time.Time) error {
	table := b.tables.Spans
	w, err := openBackupWriter(dir, "traces")
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

		attribute = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		scope     = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		resource  = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}

		eventsTimestamps = &proto.ColArr[time.Time]{}
		eventsNames      = &proto.ColArr[string]{}
		eventsAttributes = &proto.ColArr[[]byte]{}

		linksTraceIDs    = &proto.ColArr[otelstorage.TraceID]{}
		linksSpanIDs     = &proto.ColArr[otelstorage.SpanID]{}
		linksTraceStates = &proto.ColArr[string]{}
		linksAttributes  = &proto.ColArr[[]byte]{}
	)

	columns := MergeColumns(
		Columns{
			{Name: "trace_id", Data: &traceID},
			{Name: "span_id", Data: &spanID},
			{Name: "trace_state", Data: &traceState},
			{Name: "parent_span_id", Data: &parentSpan},

			{Name: "name", Data: name},
			{Name: "kind", Data: proto.Wrap(&kind, kindDDL)},
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
			{Name: "links_tracestates", Data: linksTraceStates},
			{Name: "links_attributes", Data: linksAttributes},
		},
		Columns{
			{Name: "attribute", Data: attribute},
			{Name: "scope", Data: scope},
			{Name: "resource", Data: resource},
		},
	)

	var buf proto.Buffer

	q := fmt.Sprintf(`SELECT
    trace_id,
    span_id,
    trace_state,
    parent_span_id,
    name,
    kind,
    start,
    end,
    status_code,
    status_message,
    batch_id,
    events_timestamps,
    events_names,
    events_attributes,
    links_trace_ids,
    links_span_ids,
    links_tracestates,
    links_attributes,
    attribute,
    scope,
    resource
FROM %s
WHERE start >= toDateTime(%d) AND start <= toDateTime(%d)`, table, start.Unix(), end.Unix())

	if err := b.client.Do(ctx, ch.Query{
		Body:   q,
		Result: columns.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			buf.Reset()
			if err := block.EncodeRawBlock(&buf, 54451, columns.Input()); err != nil {
				return errors.Wrap(err, "encode raw block")
			}
			if _, err := w.Write(buf.Buf); err != nil {
				return errors.Wrap(err, "write block")
			}
			return nil
		},
	}); err != nil {
		return err
	}
	return nil
}
