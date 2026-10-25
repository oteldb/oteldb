package profilestorage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectSeriesNumBuckets(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_000_000, 0)
	for _, tc := range []struct {
		name       string
		start, end time.Time
		step       time.Duration
		want       int
		invalid    bool
	}{
		{"aligned", base, base.Add(120 * time.Second), time.Second, 120, false},
		{"partial", base, base.Add(2500 * time.Millisecond), time.Second, 3, false},
		{"wide step", base, base.Add(time.Second), time.Minute, 1, false},
		{"zero start", time.Time{}, base, time.Second, 0, true},
		{"zero end", base, time.Time{}, time.Second, 0, true},
		{"reversed", base, base.Add(-time.Second), time.Second, 0, true},
		{"empty", base, base, time.Second, 0, true},
		{"zero step", base, base.Add(time.Second), 0, 0, true},
		{"negative step", base, base.Add(time.Second), -time.Second, 0, true},
		{"duration overflow", time.Date(1800, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), time.Second, 0, true},
		{"wrapped timestamp", time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2500, 1, 1, 1, 0, 0, 0, time.UTC), time.Second, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n, err := (SelectSeriesParams{Start: tc.start, End: tc.end, Step: tc.step}).NumBuckets()
			if tc.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, n)
			}
		})
	}
}
