// Package chotel reads ClickHouse internal OpenTelemetry spans.
package chotel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/chtrace"
)

const (
	// DefaultLag is the default delay before recent spans are eligible for reading.
	DefaultLag = 30 * time.Second
	// DefaultLookback is the default initial cursor lookback.
	DefaultLookback = 5 * time.Minute
)

// ReaderOption configures Reader.
type ReaderOption func(*Reader)

// WithLag configures how long Reader waits before reading recent spans.
func WithLag(lag time.Duration) ReaderOption {
	return func(r *Reader) {
		if lag > 0 {
			r.lag = lag
		}
	}
}

// WithLookback configures initial cursor lookback for Reader.
func WithLookback(lookback time.Duration) ReaderOption {
	return func(r *Reader) {
		if lookback > 0 {
			r.lookback = lookback
		}
	}
}

// Reader reads spans from ClickHouse system.opentelemetry_span_log.
type Reader struct {
	ch       *ch.Client
	latest   time.Time
	lag      time.Duration
	lookback time.Duration
}

// NewReader creates a new Reader.
func NewReader(client *ch.Client, opts ...ReaderOption) *Reader {
	r := &Reader{
		ch:       client,
		lag:      DefaultLag,
		lookback: DefaultLookback,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Read reads new spans.
func (r *Reader) Read(ctx context.Context, now time.Time) ([]chtrace.Trace, error) {
	cursor := r.latest
	if cursor.IsZero() {
		cursor = now.Add(-r.lookback).Add(-time.Microsecond)
	}
	windowEnd := now.Add(-r.lag)
	if !cursor.Before(windowEnd) {
		return nil, nil
	}

	t := chtrace.NewTable()
	q := fmt.Sprintf("SELECT %s FROM system.opentelemetry_span_log log ", strings.Join(t.Columns(), ", "))
	q += fmt.Sprintf(" PREWHERE finish_time_us > %d AND finish_time_us < %d", cursor.UnixMicro(), windowEnd.UnixMicro())
	q += " ORDER BY finish_time_us ASC LIMIT 10000"
	zctx.From(ctx).Debug("Selecting spans",
		zap.String("query", q),
		zap.Time("cursor", cursor),
		zap.Time("window_end", windowEnd),
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

// Advance moves Reader cursor forward.
func (r *Reader) Advance(t time.Time) {
	if !t.IsZero() && r.latest.Before(t) {
		r.latest = t
	}
}

// MaxFinishTime returns the latest finish time in spans.
func MaxFinishTime(spans []chtrace.Trace) time.Time {
	var latest time.Time
	for _, span := range spans {
		if latest.Before(span.FinishTime) {
			latest = span.FinishTime
		}
	}
	return latest
}

func noPropagation(ctx context.Context) context.Context {
	return trace.ContextWithSpanContext(ctx, trace.SpanContext{})
}
