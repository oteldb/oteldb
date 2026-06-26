package profilehandler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/pyroscopeapi"
)

func TestParseAtTime(t *testing.T) {
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		input   string
		want    time.Time
		wantErr bool
	}{
		{"", now, false},
		{"now", now, false},
		{"now-1h", now.Add(-time.Hour), false},
		{"now+30m", now.Add(30 * time.Minute), false},
		{"now-1h30m", now.Add(-90 * time.Minute), false},
		{"now-7d", now.Add(-7 * 24 * time.Hour), false},
		// Unix seconds.
		{"1609459200", time.Unix(1609459200, 0), false},
		// Unix milliseconds (13 digits).
		{"1609459200000", time.UnixMilli(1609459200000), false},
		// Unix nanoseconds (19 digits).
		{"1609459200000000000", time.Unix(0, 1609459200000000000), false},
		// Underscores and commas are stripped.
		{"1_609_459_200", time.Unix(1609459200, 0), false},

		// Errors.
		{"yesterday", time.Time{}, true},
		{"now-1x", time.Time{}, true},
		{"now-h", time.Time{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseAtTime(tt.input, now)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, got.Equal(tt.want), "got %s, want %s", got, tt.want)
		})
	}
}

func TestParseTimeRange(t *testing.T) {
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	const since = time.Hour

	t.Run("defaults", func(t *testing.T) {
		start, end, err := parseTimeRange(now, pyroscopeapi.OptAtTime{}, pyroscopeapi.OptAtTime{}, since)
		require.NoError(t, err)
		require.True(t, end.Equal(now))
		require.True(t, start.Equal(now.Add(-since)))
	})

	t.Run("explicit", func(t *testing.T) {
		start, end, err := parseTimeRange(now,
			pyroscopeapi.NewOptAtTime("now-2h"),
			pyroscopeapi.NewOptAtTime("now-1h"),
			since,
		)
		require.NoError(t, err)
		require.True(t, start.Equal(now.Add(-2*time.Hour)))
		require.True(t, end.Equal(now.Add(-time.Hour)))
	})

	t.Run("end before start", func(t *testing.T) {
		_, _, err := parseTimeRange(now,
			pyroscopeapi.NewOptAtTime("now"),
			pyroscopeapi.NewOptAtTime("now-1h"),
			since,
		)
		require.Error(t, err)
	})

	t.Run("invalid from", func(t *testing.T) {
		_, _, err := parseTimeRange(now, pyroscopeapi.NewOptAtTime("nope"), pyroscopeapi.OptAtTime{}, since)
		require.Error(t, err)
	})
}
