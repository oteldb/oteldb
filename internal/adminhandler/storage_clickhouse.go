package adminhandler

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/adminapi"
	"github.com/oteldb/oteldb/internal/chstorage"
)

// CHStorageStats collects per-table statistics from ClickHouse system tables.
type CHStorageStats struct {
	ch chstorage.ClickHouseClient
}

var _ StorageStatsCollector = (*CHStorageStats)(nil)

// NewCHStorageStats creates a ClickHouse-backed storage stats collector.
func NewCHStorageStats(client chstorage.ClickHouseClient) *CHStorageStats {
	return &CHStorageStats{ch: client}
}

// CollectStorageStats reads active-part aggregates from system.parts for every
// table in the current database.
func (c *CHStorageStats) CollectStorageStats(ctx context.Context) ([]adminapi.TableStats, error) {
	var (
		database  proto.ColStr
		table     proto.ColStr
		rows      proto.ColUInt64
		onDisk    proto.ColUInt64
		uncompr   proto.ColUInt64
		parts     proto.ColUInt64
		minTime   proto.ColDateTime
		maxTime   proto.ColDateTime
		collected []adminapi.TableStats
	)

	q := ch.Query{
		Body: `SELECT
	database,
	table,
	sum(rows) AS rows,
	sum(bytes_on_disk) AS bytes_on_disk,
	sum(data_uncompressed_bytes) AS data_uncompressed_bytes,
	count() AS parts,
	min(min_time) AS min_time,
	max(max_time) AS max_time
FROM system.parts
WHERE active AND database = currentDatabase()
GROUP BY database, table
ORDER BY table`,
		Result: proto.Results{
			{Name: "database", Data: &database},
			{Name: "table", Data: &table},
			{Name: "rows", Data: &rows},
			{Name: "bytes_on_disk", Data: &onDisk},
			{Name: "data_uncompressed_bytes", Data: &uncompr},
			{Name: "parts", Data: &parts},
			{Name: "min_time", Data: &minTime},
			{Name: "max_time", Data: &maxTime},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			for i := 0; i < block.Rows; i++ {
				ts := adminapi.TableStats{
					Database:              database.Row(i),
					Table:                 table.Row(i),
					Rows:                  int64(rows.Row(i)),
					BytesOnDisk:           int64(onDisk.Row(i)),
					DataUncompressedBytes: int64(uncompr.Row(i)),
					Parts:                 int64(parts.Row(i)),
				}
				if t := minTime.Row(i); !isZeroTime(t) {
					ts.MinTime = adminapi.NewOptDateTime(t)
				}
				if t := maxTime.Row(i); !isZeroTime(t) {
					ts.MaxTime = adminapi.NewOptDateTime(t)
				}
				collected = append(collected, ts)
			}
			return nil
		},
	}
	if err := c.ch.Do(ctx, q); err != nil {
		return nil, errors.Wrap(err, "query system.parts")
	}
	return collected, nil
}

// isZeroTime reports whether t is the ClickHouse "unset" DateTime (the Unix epoch), which
// system.parts reports for tables whose partition key carries no time information.
func isZeroTime(t time.Time) bool {
	return t.Unix() <= 0
}
