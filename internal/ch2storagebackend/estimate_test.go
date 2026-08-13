package ch2storagebackend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/chstorage"
)

// weekEstimate is the shape that motivated the pre-flight report: seven days of production
// metrics, ~2.8B points each.
func weekEstimate() Estimate {
	est := Estimate{
		Signal:        SignalMetrics,
		From:          day("2026-08-05"),
		To:            day("2026-08-12"),
		RowsPerSecond: DefaultMetricsRowsPerSecond,
		Size: chstorage.TableSize{
			Rows:              20_000_000_000,
			CompressedBytes:   80_000_000_000,
			UncompressedBytes: 680_000_000_000,
		},
	}
	for i, rows := range []uint64{
		2_740_237_809, 2_750_850_075, 2_800_000_000,
		2_810_000_000, 2_820_000_000, 2_830_000_000, 2_950_209_213,
	} {
		est.Days = append(est.Days, EstimateDay{Day: day("2026-08-05").AddDate(0, 0, i), Rows: rows})
		est.Rows += rows
		est.Remaining += rows
	}
	return est
}

func TestEstimateDuration(t *testing.T) {
	t.Run("projects from the remaining rows", func(t *testing.T) {
		est := weekEstimate()
		// ~19.7B points at 1.5M/s is hours, not minutes — the distinction the report exists to make.
		assert.Greater(t, est.Duration(), 3*time.Hour)
		assert.Less(t, est.Duration(), 5*time.Hour)
	})

	t.Run("checkpointed days do not count", func(t *testing.T) {
		full := weekEstimate()

		partial := weekEstimate()
		partial.Remaining = 0
		for i := range partial.Days {
			if i < 5 {
				partial.Days[i].Skipped = true
				continue
			}
			partial.Remaining += partial.Days[i].Rows
		}

		assert.Less(t, partial.Duration(), full.Duration()/2)
	})

	t.Run("zero throughput does not divide by zero", func(t *testing.T) {
		est := weekEstimate()
		est.RowsPerSecond = 0
		assert.Zero(t, est.Duration())
	})
}

func TestEstimateBytes(t *testing.T) {
	t.Run("scales the source averages by remaining rows", func(t *testing.T) {
		est := weekEstimate()
		compressed, uncompressed := est.Bytes()

		// The whole table averages 4 compressed / 34 uncompressed bytes per row.
		assert.InDelta(t, float64(est.Remaining)*4, float64(compressed), float64(compressed)*0.01)
		assert.InDelta(t, float64(est.Remaining)*34, float64(uncompressed), float64(uncompressed)*0.01)

		// The pair must stay distinguishable: quoting one as "the size" is the confusion the
		// report is meant to remove.
		assert.Greater(t, uncompressed, compressed*5)
	})

	t.Run("empty source table", func(t *testing.T) {
		compressed, uncompressed := Estimate{Remaining: 100}.Bytes()
		assert.Zero(t, compressed)
		assert.Zero(t, uncompressed)
	})
}

func TestEstimateString(t *testing.T) {
	t.Run("nothing to migrate", func(t *testing.T) {
		out := Estimate{Signal: SignalLogs}.String()
		assert.Contains(t, out, "nothing to migrate")
	})

	t.Run("reports both size units and the assumed rate", func(t *testing.T) {
		out := weekEstimate().String()

		assert.Contains(t, out, "metrics")
		assert.Contains(t, out, "2026-08-05")
		assert.Contains(t, out, "2,740,237,809", "day counts are separated for legibility")
		assert.Contains(t, out, "compressed")
		assert.Contains(t, out, "uncompressed")
		assert.Contains(t, out, "rows/s", "the projection states the rate it assumes")
	})

	t.Run("marks and excludes checkpointed days", func(t *testing.T) {
		est := weekEstimate()
		est.Days[0].Skipped = true
		est.Remaining -= est.Days[0].Rows

		out := est.String()
		assert.Contains(t, out, "(done)")
		assert.Contains(t, out, "remaining")
	})
}

func TestRowsPerSecond(t *testing.T) {
	assert.Equal(t, float64(DefaultMetricsRowsPerSecond), RowsPerSecond(SignalMetrics))
	assert.Equal(t, float64(DefaultTracesRowsPerSecond), RowsPerSecond(SignalTraces))
	assert.Equal(t, float64(DefaultLogsRowsPerSecond), RowsPerSecond(SignalLogs))
}

func TestFormatCount(t *testing.T) {
	tests := []struct {
		in   uint64
		want string
	}{
		{0, "0"},
		{7, "7"},
		{999, "999"},
		{1000, "1,000"},
		{12_345, "12,345"},
		{123_456, "123,456"},
		{1_234_567, "1,234,567"},
		{2_740_237_809, "2,740,237,809"},
		{19_700_000_000, "19,700,000,000"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, formatCount(tt.in))
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		in   uint64
		want string
	}{
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1 << 20, "1.0 MiB"},
		{1 << 30, "1.0 GiB"},
		{670 << 30, "670.0 GiB"},
		{1 << 40, "1.0 TiB"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, formatBytes(tt.in))
	}
}

func TestFormatDuration(t *testing.T) {
	assert.Equal(t, "45s", formatDuration(45*time.Second))
	assert.Equal(t, "5.0m", formatDuration(5*time.Minute))
	assert.Equal(t, "3.5h", formatDuration(3*time.Hour+30*time.Minute))
}

func TestPlan(t *testing.T) {
	var (
		mint = day("2026-08-05")
		maxt = day("2026-08-11")
	)

	t.Run("empty source", func(t *testing.T) {
		_, _, _, ok := plan(chstorage.Window{}, time.Time{}, time.Time{})
		assert.False(t, ok)
	})

	t.Run("window selects nothing", func(t *testing.T) {
		_, _, _, ok := plan(chstorage.Window{From: day("2026-09-01")}, mint, maxt)
		assert.False(t, ok)
	})

	t.Run("returns one bucket per day", func(t *testing.T) {
		days, from, to, ok := plan(chstorage.Window{From: day("2026-08-08")}, mint, maxt)
		require.True(t, ok)
		assert.Equal(t, day("2026-08-08"), from)
		// The source's maxt is floored to whole seconds, so plan widens it to that second's end
		// rather than clamping the last bucket short of the rows it was floored from.
		assert.Equal(t, chstorage.EndOfSecond(maxt), to)
		assert.Len(t, days, 4)
	})

	// An explicit upper bound is exact and must survive to the last bucket. It used to be dropped
	// at day granularity, so `-to 14:00` scanned and ingested the whole calendar day.
	t.Run("honors an explicit upper bound", func(t *testing.T) {
		to := ts("2026-08-08 14:00:00")
		days, _, gotTo, ok := plan(chstorage.Window{From: day("2026-08-07"), To: to}, mint, maxt)
		require.True(t, ok)
		assert.Equal(t, to, gotTo)
		require.Len(t, days, 2)
		assert.Equal(t, day("2026-08-07"), days[0].From)
		assert.Equal(t, day("2026-08-08"), days[0].To)
		assert.Equal(t, day("2026-08-08"), days[1].From)
		assert.Equal(t, to, days[1].To)
	})
}
