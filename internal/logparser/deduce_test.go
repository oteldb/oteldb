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

var parseSourceTests = []struct {
	input   string
	want    Source
	wantErr string
}{
	{`hello.go:1:2`, Source{Filename: "hello.go", Line: 1, Column: 2}, ``},
	{`  hello.go:1:2` + "\n", Source{Filename: "hello.go", Line: 1, Column: 2}, ``},
	{`chstorage/querier.go:157`, Source{Filename: "chstorage/querier.go", Line: 157, Column: 0}, ``},
	{`chstorage/querier.go:157:10`, Source{Filename: "chstorage/querier.go", Line: 157, Column: 10}, ``},

	{`hello.go:10:a`, Source{}, `parse column number: strconv.Atoi: parsing "a": invalid syntax`},
	{`hello.go:a`, Source{}, `parse line number: strconv.Atoi: parsing "a": invalid syntax`},

	{`:1:2`, Source{}, `filename is empty`},
	{`:1`, Source{}, `filename is empty`},
	{`:`, Source{}, `filename is empty`},
	{``, Source{}, `a ':' expected`},
}

func TestParseSource(t *testing.T) {
	for i, tt := range parseSourceTests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			t.Logf("Input: %#q", tt.input)

			got, gotErr := ParseSource(tt.input)
			if tt.wantErr != "" {
				require.EqualError(t, gotErr, tt.wantErr)
				return
			}
			require.NoError(t, gotErr)
			require.Equal(t, tt.want, got)
		})
	}
}

func FuzzParseSource(f *testing.F) {
	for _, tt := range parseSourceTests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		s, err := ParseSource(input)
		if err != nil {
			return
		}
		if s.Filename == "" {
			t.Fatal("filename must not be empty")
		}
	})
}
