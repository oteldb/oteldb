package logparser

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestDeduceNanos(t *testing.T) {
	var (
		start = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		end   = time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)
	)
	truncate := func(nanos int64, zeroes int) int64 {
		d := int64(math.Pow10(zeroes))
		return nanos - nanos%d
	}
	assert := func(a, b int64, msgAndArgs ...interface{}) {
		t.Helper()
		v, ok := DeduceNanos(a)
		require.True(t, ok, msgAndArgs...)
		require.Equal(t, b, v, msgAndArgs...)
	}
	for v := start; v.Before(end); v = v.Add(time.Second*44 + time.Nanosecond*1337123 + time.Hour*6) {
		expected := v.UnixNano()
		assert(v.Unix(), truncate(expected, 9), "v=%v", v)
		assert(v.UnixMilli(), truncate(expected, 6), "v=%v", v)
		assert(v.UnixMicro(), truncate(expected, 3), "v=%v", v)
		assert(v.UnixNano(), expected, "v=%v", v)
	}
}

func TestDeduceSeverity(t *testing.T) {
	tests := []struct {
		text string
		want plog.SeverityNumber
	}{
		{text: "", want: plog.SeverityNumberUnspecified},
		{text: "i", want: plog.SeverityNumberInfo},
		{text: "d", want: plog.SeverityNumberDebug},
		{text: "TRACE", want: plog.SeverityNumberTrace},
		{text: "trace", want: plog.SeverityNumberTrace},
		{text: "info", want: plog.SeverityNumberInfo},
		{text: "warn", want: plog.SeverityNumberWarn},
		{text: "warning", want: plog.SeverityNumberWarn},
		{text: "error", want: plog.SeverityNumberError},
		{text: "fatal", want: plog.SeverityNumberFatal},
		{text: "CRITICAL", want: plog.SeverityNumberFatal},
		{text: "deBug", want: plog.SeverityNumberDebug},
		{text: " deBug ", want: plog.SeverityNumberDebug},

		{text: "my-custom-log-level", want: plog.SeverityNumberUnspecified},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, DeduceSeverity(tt.text))
		})
	}
}
