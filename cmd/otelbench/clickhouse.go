package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
)

type ClickhouseStats struct {
	Start            time.Time
	Latest           time.Time
	Delta            time.Duration
	Rows             int
	DiskSizeBytes    int
	PrimaryKeySize   int
	CompressedSize   int
	UncompressedSize int
	CompressRatio    float64
	PointsPerSecond  int
}

func (v ClickhouseStats) WriteInfo(b *strings.Builder, now time.Time) {
	b.WriteString(" ")
	if v.Start.IsZero() {
		b.WriteString("uptime=N/A")
	} else {
		fmt.Fprintf(b, "uptime=%s", now.Sub(v.Start).Round(time.Second))
	}
	b.WriteString(" ")
	if v.Delta != -1 {
		fmt.Fprintf(b, "lag=%s", v.Delta.Round(time.Second))
	} else {
		b.WriteString("lag=N/A")
	}
	b.WriteString(" ")
	fmt.Fprintf(b, "pps=%s", fmtInt(v.PointsPerSecond))
	b.WriteString(" ")
	fmt.Fprintf(b, "rows=%s", fmtInt(v.Rows))
	b.WriteString(" ")
	fmt.Fprintf(b, "%s -> %s (%.0fx)",
		compactBytes(v.CompressedSize),
		compactBytes(v.UncompressedSize),
		v.CompressRatio,
	)
	bytesPerPoint := float64(v.CompressedSize) / float64(v.Rows)
	b.WriteString(" ")
	if v.Rows == 0 {
		b.WriteString("N/A b/point")
	} else {
		fmt.Fprintf(b, "%.1f b/point", bytesPerPoint)
	}

	type metric struct {
		Name    string
		Seconds int
	}
	if v.Rows > 0 {
		for _, m := range []metric{
			{Name: "d", Seconds: 60 * 60 * 24},
			{Name: "w", Seconds: 60 * 60 * 24 * 7},
			{Name: "m", Seconds: 60 * 60 * 24 * 30},
		} {
			rowsPerPeriod := v.PointsPerSecond * m.Seconds
			dataPerPeriod := float64(rowsPerPeriod) / float64(v.Rows) * float64(v.CompressedSize)
			b.WriteString(" ")
			fmt.Fprintf(b, "%s/%s", compactBytes(int(dataPerPeriod)), m.Name)
		}
	}
}

func fetchClickhouseStats(ctx context.Context, addr, tableName string) (info ClickhouseStats, _ error) {
	client, err := ch.Dial(ctx, ch.Options{
		Address: addr,
	})
	if err != nil {
		return info, errors.Wrap(err, "dial")
	}
	defer func() {
		_ = client.Close()
	}()
	{
		var start proto.ColDateTime64
		if err := client.Do(ctx, ch.Query{
			Body: fmt.Sprintf(`SELECT min(timestamp) as start FROM %s`, tableName),
			Result: proto.Results{
				{Name: "start", Data: &start},
			},
		}); err != nil {
			return info, errors.Wrap(err, "query")
		}
		info.Start = start.Row(0)
	}
	{
		var (
			seconds proto.ColDateTime
			delta   proto.ColInt32
			points  proto.ColUInt64
		)
		// Select aggregated points per second for last 100 seconds.
		if err := client.Do(ctx, ch.Query{
			Body: fmt.Sprintf(`SELECT toDateTime(toStartOfSecond(timestamp)) as ts, (now() - toDateTime(ts)) as delta, COUNT() as total
FROM %s
WHERE timestamp > (now() - toIntervalSecond(100))
GROUP BY ts
HAVING total > 0
ORDER BY ts DESC
LIMIT 100`, tableName),
			Result: proto.Results{
				{Name: "ts", Data: &seconds},
				{Name: "delta", Data: &delta},
				{Name: "total", Data: &points},
			},
		}); err != nil {
			return info, errors.Wrap(err, "query")
		}
		if len(points) > 0 {
			info.PointsPerSecond = int(slices.Max(points))
			info.Delta = time.Duration(slices.Min(delta)) * time.Second
		} else {
			info.PointsPerSecond = 0
			info.Delta = -1
		}
		for i := 0; i < points.Rows(); i++ {
			ts := seconds.Row(i)
			v := points.Row(i)
			if v == 0 {
				continue
			}
			if ts.After(info.Latest) {
				info.Latest = ts
			}
		}
	}
	{
		var (
			table            proto.ColStr
			rows             proto.ColUInt64
			diskSize         proto.ColUInt64
			primaryKeysSize  proto.ColUInt64
			compressedSize   proto.ColUInt64
			uncompressedSize proto.ColUInt64
			compressRatio    proto.ColFloat64
		)
		const query = `select parts.*,
       columns.compressed_size,
       columns.uncompressed_size,
       columns.ratio
from (
         select table,
                sum(data_uncompressed_bytes)    AS uncompressed_size,
                sum(data_compressed_bytes)      AS compressed_size,
                round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
         from system.columns
         where database = 'default'
         group by table
         ) columns
         right join (
    select table,
           sum(rows)                        as rows,
           sum(bytes)                       as disk_size,
           sum(primary_key_bytes_in_memory) as primary_keys_size
    from system.parts
    where active and database = 'default'
    group by database, table
    ) parts on columns.table = parts.table
order by parts.disk_size desc`
		if err := client.Do(ctx, ch.Query{
			Body: query,
			Result: proto.Results{
				{Name: "parts.table", Data: &table},
				{Name: "rows", Data: &rows},
				{Name: "disk_size", Data: &diskSize},
				{Name: "primary_keys_size", Data: &primaryKeysSize},
				{Name: "compressed_size", Data: &compressedSize},
				{Name: "uncompressed_size", Data: &uncompressedSize},
				{Name: "ratio", Data: &compressRatio},
			},
		}); err != nil {
			return info, errors.Wrap(err, "query")
		}
		for i := 0; i < len(rows); i++ {
			switch table.Row(i) {
			case tableName:
				info.Rows = int(rows.Row(i))
				info.DiskSizeBytes = int(diskSize.Row(i))
				info.PrimaryKeySize = int(primaryKeysSize.Row(i))
				info.CompressedSize = int(compressedSize.Row(i))
				info.UncompressedSize = int(uncompressedSize.Row(i))
				info.CompressRatio = compressRatio.Row(i)
			default:
				continue
			}
		}
	}
	return info, nil
}
