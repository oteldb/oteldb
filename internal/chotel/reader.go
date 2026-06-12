// Package chotel reads ClickHouse internal OpenTelemetry spans.
package chotel

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/chtrace"
)

// ExportTableDDL creates table tracking spans exported from ClickHouse.
const ExportTableDDL = `CREATE TABLE IF NOT EXISTS opentelemetry_span_export
(
    trace_id    UUID,
    span_id     UInt64,
    exported_at DATETIME
)
    ENGINE = MergeTree
        ORDER BY (toStartOfMinute(exported_at), trace_id, span_id)
        TTL toStartOfMinute(exported_at) + INTERVAL 10 MINUTE
`

// Reader reads spans from ClickHouse system.opentelemetry_span_log.
type Reader struct {
	ch     *ch.Client
	latest time.Time
}

// NewReader creates a new Reader.
func NewReader(client *ch.Client) *Reader {
	return &Reader{ch: client}
}

// Setup creates export tracking table.
func (r *Reader) Setup(ctx context.Context) error {
	if err := r.ch.Do(ctx, ch.Query{Body: ExportTableDDL}); err != nil {
		return errors.Wrap(err, "ensure export table")
	}
	return nil
}

// Read reads new spans.
func (r *Reader) Read(ctx context.Context, now time.Time) ([]chtrace.Trace, error) {
	t := chtrace.NewTable()
	q := fmt.Sprintf("SELECT %s FROM system.opentelemetry_span_log log ", strings.Join(t.Columns(), ", "))
	q += " ANTI JOIN opentelemetry_span_export ose ON log.trace_id = ose.trace_id AND log.span_id = ose.span_id"
	if !r.latest.IsZero() {
		q += fmt.Sprintf(" PREWHERE start_time_us > %d", r.latest.Add(time.Minute).UnixMicro())
	}
	q += " ORDER BY log.start_time_us DESC LIMIT 10000"
	zctx.From(ctx).Debug("Selecting spans",
		zap.String("query", q),
		zap.Time("time", r.latest),
	)

	var spans []chtrace.Trace
	if err := r.ch.Do(noPropagation(ctx), ch.Query{
		Body:   q,
		Result: t.Result(),
		OnResult: func(context.Context, proto.Block) error {
			for row := range t.Rows() {
				spans = append(spans, row)
			}
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}
	zctx.From(ctx).Info("Read spans",
		zap.Int("count", len(spans)),
		zap.String("latest_time", r.latest.String()),
	)
	return spans, nil
}

// MarkExported records spans as successfully exported.
func (r *Reader) MarkExported(ctx context.Context, spans []chtrace.Trace, now time.Time) error {
	if len(spans) == 0 {
		return nil
	}
	var exported struct {
		TraceID    proto.ColUUID
		SpanID     proto.ColUInt64
		ExportedAt proto.ColDateTime
	}
	var latest time.Time
	for _, span := range spans {
		exported.TraceID.Append(uuid.UUID(span.TraceID))
		exported.SpanID.Append(binary.BigEndian.Uint64(span.SpanID[:]))
		exported.ExportedAt.Append(now)
		if latest.Before(span.FinishTime) {
			latest = span.FinishTime
		}
	}
	if err := r.ch.Do(noPropagation(ctx), ch.Query{
		Body: "INSERT INTO opentelemetry_span_export (trace_id, span_id, exported_at) VALUES",
		Input: proto.Input{
			{Name: "trace_id", Data: exported.TraceID},
			{Name: "span_id", Data: exported.SpanID},
			{Name: "exported_at", Data: exported.ExportedAt},
		},
	}); err != nil {
		return errors.Wrap(err, "insert")
	}
	if !latest.IsZero() {
		r.latest = latest
	}
	return nil
}

func noPropagation(ctx context.Context) context.Context {
	return trace.ContextWithSpanContext(ctx, trace.SpanContext{})
}
