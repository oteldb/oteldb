package logparser

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/oteldb/internal/otelstorage"
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

func TestParseTimestamp(t *testing.T) {
	d := func(year int, month time.Month, day, hour, minute, sec, nsec int, loc *time.Location) otelstorage.Timestamp {
		ts := time.Date(year, month, day, hour, minute, sec, nsec, loc)
		return otelstorage.NewTimestampFromTime(ts)
	}
	for i, tt := range []struct {
		input   string
		epsilon float64
		want    otelstorage.Timestamp
		wantOk  bool
	}{
		{
			`2026-01-20T01:02:03.000000004Z`,
			0,
			d(2026, time.January, 20, 1, 2, 3, 4, time.UTC),
			true,
		},
		{
			`2026-01-20T01:02:03.00000004Z`,
			0,
			d(2026, time.January, 20, 1, 2, 3, 40, time.UTC),
			true,
		},
		{
			`2026-01-20T01:02:03Z`,
			0,
			d(2026, time.January, 20, 1, 2, 3, 0, time.UTC),
			true,
		},
		{
			`1772058997`,
			0,
			otelstorage.NewTimestampFromTime(time.Unix(1772058997, 0)),
			true,
		},
		{
			`  1772058997` + "\n",
			0,
			otelstorage.NewTimestampFromTime(time.Unix(1772058997, 0)),
			true,
		},
		{
			`1772058997123`,
			0,
			otelstorage.NewTimestampFromTime(time.UnixMilli(1772058997_123)),
			true,
		},
		{
			`1772058997123456`,
			0,
			otelstorage.NewTimestampFromTime(time.UnixMicro(1772058997_123456)),
			true,
		},
		{
			`1772058997123456789`,
			0,
			otelstorage.NewTimestampFromTime(time.Unix(0, 1772058997_123456789)),
			true,
		},

		// Floating point.
		{
			`1772058997.123`,
			0.00001,
			otelstorage.NewTimestampFromTime(time.UnixMilli(1772058997_123)),
			true,
		},
		{
			`1701681119.1613183`,
			0.00001,
			otelstorage.NewTimestampFromTime(time.Unix(0, 1701681119_161318300)),
			true,
		},
		{
			`1772058997123.0`,
			0,
			otelstorage.NewTimestampFromTime(time.UnixMilli(1772058997_123)),
			true,
		},
		{
			`1772058997.123456`,
			0.00001,
			otelstorage.NewTimestampFromTime(time.UnixMicro(1772058997_123456)),
			true,
		},
		{
			`1772058997123456789.0`,
			0,
			otelstorage.NewTimestampFromTime(time.Unix(0, 1772058997_123456789)),
			true,
		},

		{` `, 0, 0, false},
		{``, 0, 0, false},
	} {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			t.Logf("Input: %#q", tt.input)

			got, gotOk := ParseTimestamp([]byte(tt.input))
			if !tt.wantOk {
				require.False(t, gotOk)
				return
			}
			require.True(t, gotOk)
			require.InEpsilon(t, tt.want.AsTime().UnixNano(), got.AsTime().UnixNano(), tt.epsilon)
		})
	}
}

func BenchmarkParseTimestamp(b *testing.B) {
	bench := func(input string) func(b *testing.B) {
		return func(b *testing.B) {
			data := []byte(input)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				_, ok := ParseTimestamp(data)
				if !ok {
					b.Fatal("Must be valid timestamp")
				}
			}
		}
	}

	b.Run("RFC3339Nano", bench(`2026-01-20T01:02:03.000000004Z`))
	b.Run("IntUnix", bench(`1772058997`))
	b.Run("FloatUnix", bench(`1772058997.123`))
}

var parseTraceIDTests = []struct {
	input  string
	want   string
	wantOk bool
}{
	{`0ab78e08df6f20dc3ad29d3915beab75`, `0ab78e08df6f20dc3ad29d3915beab75`, true},
	{`0AB78E08DF6F20DC3AD29D3915BEAB75`, `0ab78e08df6f20dc3ad29d3915beab75`, true},
	{`0ab78e08df6f20dc3AD29D3915BEAB75`, `0ab78e08df6f20dc3ad29d3915beab75`, true},

	{`0ab78e08-df6f-20dc-3ad2-9d3915beab75`, `0ab78e08df6f20dc3ad29d3915beab75`, true},
	{`0AB78e08-df6f-20dc-3ad2-9d3915beab75`, `0ab78e08df6f20dc3ad29d3915beab75`, true},

	{``, ``, false},
	{` `, ``, false},
	{strings.Repeat("z", 32), ``, false},
	{strings.Repeat("1", 33), ``, false},
	{strings.Repeat("z", 36), ``, false},
	{`zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz`, ``, false},
}

func TestParseTraceID(t *testing.T) {
	for i, tt := range parseTraceIDTests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			t.Logf("Input: %#q", tt.input)

			got, gotOk := ParseTraceID(tt.input)
			if !tt.wantOk {
				require.False(t, gotOk)
				return
			}
			require.True(t, gotOk)
			require.Equal(t, tt.want, got.Hex())
		})
	}
}

func FuzzParseTraceID(f *testing.F) {
	for _, tt := range parseTraceIDTests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		s, ok := ParseTraceID(input)
		if !ok {
			return
		}
		got, ok := ParseTraceID(s.Hex())
		require.True(t, ok)
		require.Equal(t, s, got)
	})
}

func BenchmarkParseTraceID(b *testing.B) {
	bench := func(input string) func(b *testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				_, ok := ParseTraceID(input)
				if !ok {
					b.Fatal("Must be valid timestamp")
				}
			}
		}
	}

	b.Run("LowerHex", bench(`0ab78e08df6f20dc3ad29d3915beab75`))
	b.Run("UpperHex", bench(`0AB78E08DF6F20DC3AD29D3915BEAB75`))
	b.Run("MixedHex", bench(`0ab78e08df6f20dc3AD29D3915BEAB75`))
	b.Run("UUID", bench(`0ab78e08-df6f-20dc-3ad2-9d3915beab75`))
}

var parseSpanIDTests = []struct {
	input  string
	want   string
	wantOk bool
}{
	{`0ab78e08df6f20dc`, `0ab78e08df6f20dc`, true},
	{`0AB78E08DF6F20DC`, `0ab78e08df6f20dc`, true},
	{`0ab78e08DF6f20DC`, `0ab78e08df6f20dc`, true},

	{``, ``, false},
	{` `, ``, false},
	{strings.Repeat("z", 16), ``, false},
	{strings.Repeat("1", 17), ``, false},
}

func TestParseSpanID(t *testing.T) {
	for i, tt := range parseSpanIDTests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			t.Logf("Input: %#q", tt.input)

			got, gotOk := ParseSpanID(tt.input)
			if !tt.wantOk {
				require.False(t, gotOk)
				return
			}
			require.True(t, gotOk)
			require.Equal(t, tt.want, got.Hex())
		})
	}
}

func FuzzParseSpanID(f *testing.F) {
	for _, tt := range parseSpanIDTests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		s, ok := ParseSpanID(input)
		if !ok {
			return
		}
		got, ok := ParseSpanID(s.Hex())
		require.True(t, ok)
		require.Equal(t, s, got)
	})
}

func BenchmarkParseSpanID(b *testing.B) {
	bench := func(input string) func(b *testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				_, ok := ParseSpanID(input)
				if !ok {
					b.Fatal("Must be valid timestamp")
				}
			}
		}
	}

	b.Run("LowerHex", bench(`0ab78e08df6f20dc`))
	b.Run("UpperHex", bench(`0AB78E08DF6F20DC`))
	b.Run("MixedHex", bench(`0ab78e08DF6f20DC`))
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
