package chstorage

import (
	"context"
	"slices"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
)

// Window bounds a migration scan in time. The zero Window covers the source table's whole range.
//
// From/To are absolute bounds. Since is an alternative lower bound, used only when From is zero:
// it starts the scan Since before the effective upper bound, which expresses "the last 24h"
// without having to know the table's max timestamp up front.
type Window struct {
	From  time.Time
	To    time.Time
	Since time.Duration
}

// Resolve intersects w with a source's [mint, maxt] range and returns the effective scan bounds.
// ok is false when the intersection is empty, meaning there is nothing to scan.
func (w Window) Resolve(mint, maxt time.Time) (from, to time.Time, ok bool) {
	from, to = mint, maxt
	if !w.To.IsZero() && w.To.Before(to) {
		to = w.To
	}
	switch {
	case !w.From.IsZero():
		if w.From.After(from) {
			from = w.From
		}
	case w.Since > 0:
		if cut := to.Add(-w.Since); cut.After(from) {
			from = cut
		}
	}
	if from.After(to) {
		return from, to, false
	}
	return from, to, true
}

// DayRange is one UTC-day-aligned scan bucket. Day is the day's start (the stable identity a
// migration checkpoints against); From/To are the bounds actually scanned, which may be narrower
// than the whole day at the edges of the requested window.
type DayRange struct {
	Day  time.Time
	From time.Time
	To   time.Time
}

// Days returns the UTC-day-aligned buckets covering [mint, maxt]. Day-aligning keeps each scan
// inside a single daily table partition; the first bucket's lower bound is clamped to mint so a
// windowed scan skips the early part of its first day instead of scanning the whole calendar day.
//
// Only the lower bound is clamped. The upper bound stays at the day's end because mint/maxt come
// from queryMinMaxTimestamp, which floors to whole seconds (toDateTime): clamping the top to a
// floored maxt would drop any sub-second data in maxt's final second. Since no data exists past the
// true max, a full-day upper bound reads the same rows without that risk.
func Days(mint, maxt time.Time) []DayRange {
	const step = 24 * time.Hour

	var (
		out   []DayRange
		start = mint.Truncate(step)
		end   = maxt.Truncate(step).Add(step)
	)
	for ts := start; ts.Before(end); ts = ts.Add(step) {
		from, to := ts, ts.Add(step)
		if from.Before(mint) {
			from = mint
		}
		out = append(out, DayRange{Day: ts, From: from, To: to})
	}
	return out
}

// DayCount is the row count of one UTC day of a source table.
type DayCount struct {
	Day  time.Time
	Rows uint64
}

// mergeDayCounts sums per-day counts from several tables into one ascending series.
func mergeDayCounts(sets ...[]DayCount) []DayCount {
	total := map[time.Time]uint64{}
	for _, set := range sets {
		for _, c := range set {
			total[c.Day] += c.Rows
		}
	}

	out := make([]DayCount, 0, len(total))
	for day, rows := range total {
		out = append(out, DayCount{Day: day, Rows: rows})
	}
	slices.SortFunc(out, func(a, b DayCount) int { return a.Day.Compare(b.Day) })
	return out
}

// dayCounts groups a table's rows in [from, to] by UTC day. It is the pre-flight sizing query: a
// grouped count reads only the timestamp column, so it is orders of magnitude cheaper than the
// scan it estimates.
func dayCounts(
	ctx context.Context,
	client ClickHouseClient,
	table, column string,
	prec proto.Precision,
	from, to time.Time,
) ([]DayCount, error) {
	var (
		dayCol   proto.ColDateTime
		countCol proto.ColUInt64
		day      = chsql.Function("toStartOfDay", chsql.Ident(column))
	)

	query := chsql.Select(table,
		chsql.ResultColumn{Name: "day", Expr: day, Data: &dayCol},
		chsql.ResultColumn{Name: "rows", Expr: chsql.Count(), Data: &countCol},
	).
		Where(chsql.InTimeRange(column, from, to, prec)).
		GroupBy(day).
		Order(chsql.Ident("day"), chsql.Asc)

	var out []DayCount
	chq, err := query.Prepare(func(ctx context.Context, block proto.Block) error {
		for i := range dayCol.Rows() {
			out = append(out, DayCount{
				Day:  dayCol.Row(i).UTC(),
				Rows: countCol.Row(i),
			})
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "prepare query")
	}
	if err := client.Do(ctx, chq); err != nil {
		return nil, errors.Wrap(err, "execute query")
	}
	return out, nil
}

// TableSize is a source table's on-disk footprint, as ClickHouse accounts it over active parts.
//
// Compressed is what the data occupies today; Uncompressed is the logical size of the same rows.
// The two differ by an order of magnitude on telemetry, so a migration estimate must say which one
// it is quoting — and neither predicts what the *target* engine will write, since it applies its
// own codecs.
type TableSize struct {
	Rows              uint64
	CompressedBytes   uint64
	UncompressedBytes uint64
}

// BytesPerRow returns the table's average compressed and uncompressed bytes per row, or zeroes
// when the table is empty.
func (s TableSize) BytesPerRow() (compressed, uncompressed float64) {
	if s.Rows == 0 {
		return 0, 0
	}
	return float64(s.CompressedBytes) / float64(s.Rows), float64(s.UncompressedBytes) / float64(s.Rows)
}

// tableSize reads a table's active-part totals from system.parts. The table name is matched within
// the connection's current database, so it works regardless of which database the DSN selects.
func tableSize(ctx context.Context, client ClickHouseClient, table string) (TableSize, error) {
	var (
		rows         proto.ColUInt64
		compressed   proto.ColUInt64
		uncompressed proto.ColUInt64
	)

	sum := func(column string) chsql.Expr {
		return chsql.Function("sum", chsql.Ident(column))
	}
	query := chsql.Select("system.parts",
		chsql.ResultColumn{Name: "rows", Expr: sum("rows"), Data: &rows},
		chsql.ResultColumn{Name: "compressed", Expr: sum("data_compressed_bytes"), Data: &compressed},
		chsql.ResultColumn{Name: "uncompressed", Expr: sum("data_uncompressed_bytes"), Data: &uncompressed},
	).
		Where(
			chsql.Eq(chsql.Ident("database"), chsql.Function("currentDatabase")),
			chsql.Eq(chsql.Ident("table"), chsql.String(table)),
			chsql.Ident("active"),
		)

	var out TableSize
	chq, err := query.Prepare(func(ctx context.Context, block proto.Block) error {
		if rows.Rows() == 0 {
			return nil
		}
		out = TableSize{
			Rows:              rows.Row(0),
			CompressedBytes:   compressed.Row(0),
			UncompressedBytes: uncompressed.Row(0),
		}
		return nil
	})
	if err != nil {
		return out, errors.Wrap(err, "prepare query")
	}
	if err := client.Do(ctx, chq); err != nil {
		return out, errors.Wrap(err, "execute query")
	}
	return out, nil
}
