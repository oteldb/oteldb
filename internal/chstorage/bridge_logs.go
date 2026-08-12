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

// LogsBatchFunc is called with each decoded batch of records read by [LogsSource.Scan].
type LogsBatchFunc func(ctx context.Context, records []logstorage.Record) error

// LogsSource reads every log record stored in ClickHouse, decoded as [logstorage.Record],
// for migrating them into another storage engine. Unlike [Querier], it performs a full
// table scan with no selector pushdown. Callers drive it one UTC day at a time (see [Days]),
// so a single scan never buffers more than one day's worth of rows in ClickHouse — and so a
// migration can checkpoint its progress at a day boundary.
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

// Range returns the timestamp bounds of the stored records. Both are zero when the table is empty.
func (s *LogsSource) Range(ctx context.Context) (mint, maxt time.Time, _ error) {
	mint, maxt, err := queryMinMaxTimestamp(ctx, s.client, [2]string{s.table, "timestamp"})
	if err != nil {
		return mint, maxt, errors.Wrap(err, "query min/max timestamp")
	}
	return mint, maxt, nil
}

// Counts returns the per-UTC-day record counts within [from, to].
func (s *LogsSource) Counts(ctx context.Context, from, to time.Time) ([]DayCount, error) {
	return dayCounts(ctx, s.client, s.table, "timestamp", proto.PrecisionNano, from, to)
}

// Size returns the table's active-part footprint, for pre-flight sizing.
func (s *LogsSource) Size(ctx context.Context) (TableSize, error) {
	return tableSize(ctx, s.client, s.table)
}

// Scan reads the records in [from, to] in timestamp order, invoking batchFn with batches of up to
// batchSize records.
func (s *LogsSource) Scan(ctx context.Context, from, to time.Time, batchSize int, batchFn LogsBatchFunc) error {
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
		Where(chsql.InTimeRange("timestamp", from, to, proto.PrecisionNano))

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
