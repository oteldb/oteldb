package lokihandler

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/lokiapi"
)

func stringToOpt[
	S ~string,
	O interface {
		SetTo(S)
	},
](
	input string,
	opt O,
) {
	if input != "" {
		opt.SetTo(S(input))
	}
}

var defaultTime = time.Date(2000, time.January, 1, 13, 0, 59, 0, time.UTC)

func Test_parseTimeRange(t *testing.T) {
	someDate := defaultTime.Add(-time.Hour)

	tests := []struct {
		startParam string
		endParam   string
		sinceParam string

		wantStart time.Time
		wantEnd   time.Time
		wantErr   bool
	}{
		{``, ``, ``, defaultTime.Add(-6 * time.Hour), defaultTime, false},
		{``, ``, `5m`, defaultTime.Add(-5 * time.Minute), defaultTime, false},
		{``, someDate.Format(time.RFC3339Nano), ``, someDate.Add(-6 * time.Hour), someDate, false},

		// Invalid since.
		{``, ``, `a`, time.Time{}, time.Time{}, true},
		// Invalid end.
		{``, `a`, ``, time.Time{}, time.Time{}, true},
		// Invalid start.
		{`a`, ``, ``, time.Time{}, time.Time{}, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			var (
				start, end lokiapi.OptLokiTime
				since      lokiapi.OptPrometheusDuration
			)
			stringToOpt[lokiapi.LokiTime](tt.startParam, &start)
			stringToOpt[lokiapi.LokiTime](tt.endParam, &end)
			stringToOpt[lokiapi.PrometheusDuration](tt.sinceParam, &since)

			gotStart, gotEnd, err := parseTimeRange(
				defaultTime,
				start,
				end,
				since,
				6*time.Hour,
			)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, tt.wantStart.Equal(gotStart))
			require.True(t, tt.wantEnd.Equal(gotEnd))
		})
	}
}

func Test_parseTimestamp(t *testing.T) {
	someDate := time.Date(2010, time.February, 4, 3, 2, 1, 0, time.UTC)
	tests := []struct {
		input       string
		defaultTime time.Time
		want        time.Time
		wantErr     bool
	}{
		// Empty parameter, use default.
		{``, defaultTime, defaultTime, false},
		// A Unix timestamp.
		{`1688650387`, defaultTime, time.Unix(1688650387, 0), false},
		// A Unix nano timestamp.
		{`1688650387000000001`, defaultTime, time.Unix(1688650387, 1), false},
		// A floating point timestamp with fractions of second.
		//
		// .001 (1/1000) of seconds is 1ms
		{`1688650387.001`, defaultTime, time.Unix(1688650387, int64(time.Millisecond)), false},
		// RFC3339Nano.
		{someDate.Format(time.RFC3339Nano), defaultTime, someDate, false},

		{`h`, time.Time{}, time.Time{}, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := ParseTimestamp(lokiapi.LokiTime(tt.input), tt.defaultTime)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, got.Equal(tt.want))
		})
	}
}

func Test_parseDuration(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{`1.0`, time.Second, false},
		{`1h2m`, time.Hour + 2*time.Minute, false},

		{``, 0, true},
		{`a`, 0, true},
		{`h`, 0, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := ParseDuration(lokiapi.PrometheusDuration(tt.input))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
