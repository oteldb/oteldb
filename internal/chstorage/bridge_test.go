package chstorage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func day(s string) time.Time {
	t, err := time.ParseInLocation(time.DateOnly, s, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}

func ts(s string) time.Time {
	t, err := time.ParseInLocation(time.DateTime, s, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}

func TestWindowResolve(t *testing.T) {
	var (
		mint = ts("2026-08-05 04:00:00")
		maxt = ts("2026-08-11 20:00:00")
	)

	tests := []struct {
		name       string
		window     Window
		wantFrom   time.Time
		wantTo     time.Time
		wantNoData bool
	}{
		{
			name:     "zero window covers the source range",
			window:   Window{},
			wantFrom: mint,
			wantTo:   maxt,
		},
		{
			name:     "from clamps up",
			window:   Window{From: day("2026-08-08")},
			wantFrom: day("2026-08-08"),
			wantTo:   maxt,
		},
		{
			name:     "from before the source range is ignored",
			window:   Window{From: day("2026-01-01")},
			wantFrom: mint,
			wantTo:   maxt,
		},
		{
			name:     "to clamps down",
			window:   Window{To: day("2026-08-09")},
			wantFrom: mint,
			wantTo:   day("2026-08-09"),
		},
		{
			name:     "to after the source range is ignored",
			window:   Window{To: day("2027-01-01")},
			wantFrom: mint,
			wantTo:   maxt,
		},
		{
			name:     "from and to bound both ends",
			window:   Window{From: day("2026-08-07"), To: day("2026-08-09")},
			wantFrom: day("2026-08-07"),
			wantTo:   day("2026-08-09"),
		},
		{
			name:     "since counts back from the upper bound",
			window:   Window{Since: 24 * time.Hour},
			wantFrom: ts("2026-08-10 20:00:00"),
			wantTo:   maxt,
		},
		{
			// Since is relative to the *effective* upper bound, so it composes with -to.
			name:     "since composes with to",
			window:   Window{To: day("2026-08-09"), Since: 24 * time.Hour},
			wantFrom: day("2026-08-08"),
			wantTo:   day("2026-08-09"),
		},
		{
			name:     "from wins over since",
			window:   Window{From: day("2026-08-06"), Since: time.Hour},
			wantFrom: day("2026-08-06"),
			wantTo:   maxt,
		},
		{
			name:     "since longer than the range keeps the range",
			window:   Window{Since: 365 * 24 * time.Hour},
			wantFrom: mint,
			wantTo:   maxt,
		},
		{
			name:       "window entirely after the source range selects nothing",
			window:     Window{From: day("2026-09-01")},
			wantNoData: true,
		},
		{
			name:       "window entirely before the source range selects nothing",
			window:     Window{To: day("2026-01-01")},
			wantNoData: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from, to, ok := tt.window.Resolve(mint, maxt)
			if tt.wantNoData {
				assert.False(t, ok)
				return
			}
			require.True(t, ok)
			assert.Equal(t, tt.wantFrom, from, "from")
			assert.Equal(t, tt.wantTo, to, "to")
		})
	}
}

func TestDays(t *testing.T) {
	t.Run("clamps the lower bound but not the upper", func(t *testing.T) {
		days := Days(ts("2026-08-05 04:00:00"), ts("2026-08-07 20:00:00"))
		require.Len(t, days, 3)

		// First bucket starts at mint, not at midnight: a windowed scan must not re-read the
		// earlier part of its first day.
		assert.Equal(t, day("2026-08-05"), days[0].Day)
		assert.Equal(t, ts("2026-08-05 04:00:00"), days[0].From)
		assert.Equal(t, day("2026-08-06"), days[0].To)

		assert.Equal(t, day("2026-08-06"), days[1].From)
		assert.Equal(t, day("2026-08-07"), days[1].To)

		// Last bucket runs to the day's end, past maxt, because maxt is floored to whole seconds
		// upstream and clamping would drop sub-second data.
		assert.Equal(t, day("2026-08-08"), days[2].To)
	})

	t.Run("day identity is midnight even when the scan is partial", func(t *testing.T) {
		days := Days(ts("2026-08-05 04:00:00"), ts("2026-08-05 06:00:00"))
		require.Len(t, days, 1)
		assert.Equal(t, day("2026-08-05"), days[0].Day)
		assert.Equal(t, ts("2026-08-05 04:00:00"), days[0].From)
	})

	t.Run("covers every day of a week", func(t *testing.T) {
		days := Days(day("2026-08-05"), ts("2026-08-11 23:59:59"))
		require.Len(t, days, 7)
		for i, d := range days {
			assert.Equal(t, day("2026-08-05").AddDate(0, 0, i), d.Day)
		}
	})

	t.Run("buckets are contiguous", func(t *testing.T) {
		days := Days(ts("2026-08-05 04:00:00"), ts("2026-08-09 12:00:00"))
		for i := 1; i < len(days); i++ {
			assert.Equal(t, days[i-1].To, days[i].From, "gap before %s", days[i].Day)
		}
	})
}

func TestMergeDayCounts(t *testing.T) {
	merged := mergeDayCounts(
		[]DayCount{{Day: day("2026-08-06"), Rows: 2}, {Day: day("2026-08-05"), Rows: 1}},
		[]DayCount{{Day: day("2026-08-06"), Rows: 40}, {Day: day("2026-08-07"), Rows: 300}},
	)

	assert.Equal(t, []DayCount{
		{Day: day("2026-08-05"), Rows: 1},
		{Day: day("2026-08-06"), Rows: 42},
		{Day: day("2026-08-07"), Rows: 300},
	}, merged)
}

func TestTableSizeBytesPerRow(t *testing.T) {
	t.Run("empty table", func(t *testing.T) {
		compressed, uncompressed := TableSize{}.BytesPerRow()
		assert.Zero(t, compressed)
		assert.Zero(t, uncompressed)
	})

	t.Run("averages over rows", func(t *testing.T) {
		size := TableSize{Rows: 100, CompressedBytes: 400, UncompressedBytes: 3200}
		compressed, uncompressed := size.BytesPerRow()
		assert.InDelta(t, 4.0, compressed, 1e-9)
		assert.InDelta(t, 32.0, uncompressed, 1e-9)
	})
}
