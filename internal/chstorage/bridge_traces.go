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

// TracesBatchFunc is called with each decoded batch of spans read by [TracesSource.Do].
type TracesBatchFunc func(ctx context.Context, spans []tracestorage.Span) error

// TracesSource reads every span stored in ClickHouse, decoded as [tracestorage.Span], for
// migrating them into another storage engine. Unlike [Querier], it performs a full table
// scan with no selector pushdown, day-bucketed like [Backup] so a single scan never buffers
// more than one day's worth of rows in ClickHouse at a time.
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

// Do scans every span in the table, in day-bucket then start-timestamp order, invoking
// batchFn with batches of up to batchSize spans. When since is positive, the scan is
// restricted to the last since of data, relative to the table's most recent start
// timestamp, instead of the full table.
func (s *TracesSource) Do(ctx context.Context, since time.Duration, batchSize int, batchFn TracesBatchFunc) error {
	mint, maxt, err := queryMinMaxTimestamp(ctx, s.client, [2]string{s.table, "start"})
	if err != nil {
		return errors.Wrap(err, "query min/max timestamp")
	}
	if mint.IsZero() && maxt.IsZero() {
		s.logger.Info("No traces to migrate")
		return nil
	}
	if since > 0 {
		if cut := maxt.Add(-since); cut.After(mint) {
			mint = cut
		}
	}

	step := 24 * time.Hour
	start := mint.Truncate(step)
	end := maxt.Truncate(step).Add(step)
	for ts := start; ts.Before(end); ts = ts.Add(step) {
		if err := s.scanDay(ctx, ts, ts.Add(step), batchSize, batchFn); err != nil {
			return errors.Wrapf(err, "scan day %s", ts)
		}
	}
	return nil
}

func (s *TracesSource) scanDay(ctx context.Context, start, end time.Time, batchSize int, batchFn TracesBatchFunc) error {
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
		Where(chsql.InTimeRange("start", start, end, proto.PrecisionNano))

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
