package chstorage

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
	"github.com/oteldb/oteldb/internal/logstorage"
)

// LogsBatchFunc is called with each decoded batch of records read by [LogsSource.Do].
type LogsBatchFunc func(ctx context.Context, records []logstorage.Record) error

// LogsSource reads every log record stored in ClickHouse, decoded as [logstorage.Record],
// for migrating them into another storage engine. Unlike [Querier], it performs a full
// table scan with no selector pushdown, day-bucketed like [Backup] so a single scan never
// buffers more than one day's worth of rows in ClickHouse at a time.
type LogsSource struct {
	client ClickHouseClient
	table  string
	logger *zap.Logger
}

// NewLogsSource creates a new [LogsSource].
func NewLogsSource(client ClickHouseClient, tables Tables, logger *zap.Logger) *LogsSource {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &LogsSource{
		client: client,
		table:  tables.Logs,
		logger: logger,
	}
}

// Do scans every log record in the table, in day-bucket then timestamp order, invoking
// batchFn with batches of up to batchSize records. When since is positive, the scan is
// restricted to the last since of data, relative to the table's most recent timestamp,
// instead of the full table.
func (s *LogsSource) Do(ctx context.Context, since time.Duration, batchSize int, batchFn LogsBatchFunc) error {
	mint, maxt, err := queryMinMaxTimestamp(ctx, s.client, [2]string{s.table, "timestamp"})
	if err != nil {
		return errors.Wrap(err, "query min/max timestamp")
	}
	if mint.IsZero() && maxt.IsZero() {
		s.logger.Info("No logs to migrate")
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

func (s *LogsSource) scanDay(ctx context.Context, start, end time.Time, batchSize int, batchFn LogsBatchFunc) error {
	var (
		lc  = newLogColumns()
		buf []logstorage.Record
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

	query := chsql.Select(s.table, lc.ChsqlResult()...).
		Where(chsql.InTimeRange("timestamp", start, end, proto.PrecisionNano))

	chq, err := query.Prepare(func(ctx context.Context, block proto.Block) error {
		// The decoded columns accumulate across blocks (ch-go appends, it never resets),
		// so lc must be drained and reset before the next block is decoded into it.
		defer lc.Reset()
		return lc.ForEach(func(r logstorage.Record) error {
			buf = append(buf, r)
			if len(buf) >= batchSize {
				return flush(ctx)
			}
			return nil
		})
	})
	if err != nil {
		return errors.Wrap(err, "prepare query")
	}
	if err := s.client.Do(ctx, chq); err != nil {
		return errors.Wrap(err, "execute query")
	}
	return flush(ctx)
}
