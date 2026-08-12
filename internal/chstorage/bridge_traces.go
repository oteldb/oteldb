package chstorage

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// TracesBatchFunc is called with each decoded batch of spans read by [TracesSource.Scan].
type TracesBatchFunc func(ctx context.Context, spans []tracestorage.Span) error

// TracesSource reads every span stored in ClickHouse, decoded as [tracestorage.Span], for
// migrating them into another storage engine. Unlike [Querier], it performs a full table
// scan with no selector pushdown. Callers drive it one UTC day at a time (see [Days]), so a
// single scan never buffers more than one day's worth of rows in ClickHouse — and so a
// migration can checkpoint its progress at a day boundary.
type TracesSource struct {
	client ClickHouseClient
	table  string
	logger *zap.Logger
}

// NewTracesSource creates a new [TracesSource].
func NewTracesSource(client ClickHouseClient, tables Tables, logger *zap.Logger) *TracesSource {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &TracesSource{
		client: client,
		table:  tables.Spans,
		logger: logger,
	}
}

// Range returns the start-timestamp bounds of the stored spans. Both are zero when the table is
// empty.
func (s *TracesSource) Range(ctx context.Context) (mint, maxt time.Time, _ error) {
	mint, maxt, err := queryMinMaxTimestamp(ctx, s.client, [2]string{s.table, "start"})
	if err != nil {
		return mint, maxt, errors.Wrap(err, "query min/max timestamp")
	}
	return mint, maxt, nil
}

// Counts returns the per-UTC-day span counts within [from, to].
func (s *TracesSource) Counts(ctx context.Context, from, to time.Time) ([]DayCount, error) {
	return dayCounts(ctx, s.client, s.table, "start", proto.PrecisionNano, from, to)
}

// Size returns the table's active-part footprint, for pre-flight sizing.
func (s *TracesSource) Size(ctx context.Context) (TableSize, error) {
	return tableSize(ctx, s.client, s.table)
}

// Scan reads the spans in [from, to] in start-timestamp order, invoking batchFn with batches of up
// to batchSize spans.
func (s *TracesSource) Scan(ctx context.Context, from, to time.Time, batchSize int, batchFn TracesBatchFunc) error {
	var (
		sc  = newSpanColumns()
		buf []tracestorage.Span
	)

	flush := func(ctx context.Context) error {
		if len(buf) == 0 {
			return nil
		}
		if err := batchFn(ctx, buf); err != nil {
			return err
		}
		buf = buf[:0]
		return nil
	}

	query := chsql.Select(s.table, sc.ChsqlResult()...).
		Where(chsql.InTimeRange("start", from, to, proto.PrecisionNano))

	chq, err := query.Prepare(func(ctx context.Context, block proto.Block) error {
		// The decoded columns accumulate across blocks (ch-go appends, it never resets),
		// so sc must be drained and reset before the next block is decoded into it.
		defer sc.Reset()
		spans, err := sc.ReadRowsTo(nil)
		if err != nil {
			return errors.Wrap(err, "decode spans")
		}
		for _, span := range spans {
			buf = append(buf, span)
			if len(buf) >= batchSize {
				if err := flush(ctx); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "prepare query")
	}
	if err := s.client.Do(ctx, chq); err != nil {
		return errors.Wrap(err, "execute query")
	}
	return flush(ctx)
}
