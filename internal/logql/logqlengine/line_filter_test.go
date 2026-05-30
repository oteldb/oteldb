package logqlengine

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
)

func TestLineFilter(t *testing.T) {
	ip := func(pattern string) logql.LineFilterValue {
		return logql.LineFilterValue{IP: true, Value: pattern}
	}
	re := func(pattern string) logql.LineFilterValue {
		return logql.LineFilterValue{Value: pattern, Re: regexp.MustCompile(pattern)}
	}

	type line struct {
		line string
		keep bool
	}
	tests := []struct {
		inputs []line
		filter logql.LineFilter
	}{
		{
			[]line{
				{"", true},
				{"foo", true},
			},
			logql.LineFilter{
				Op: logql.OpEq,
				By: logql.LineFilterValue{Value: ""},
			},
		},
		{
			[]line{
				{"", false},
				{"foo", true},
				{"barfoo", true},
				{"bar foo", true},
				{"bar", false},
			},
			logql.LineFilter{
				Op: logql.OpEq,
				By: logql.LineFilterValue{Value: "foo"},
			},
		},

		// Regex filter.
		{
			[]line{
				{"", true},
				{"foo", true},
			},
			logql.LineFilter{
				Op: logql.OpRe,
				By: re(".*"),
			},
		},
		{
			[]line{
				{"", false},
				{"foo", true},
			},
			logql.LineFilter{
				Op: logql.OpRe,
				By: re(".+"),
			},
		},
		{
			[]line{
				{"", false},
				{"foo", true},
				{"foobar", true},
				{" foo ", true},
			},
			logql.LineFilter{
				Op: logql.OpRe,
				By: re("(foo|bar)"),
			},
		},
		{
			[]line{
				{"", false},
				{"foo", true},
				{"foobar", false},
				{" foo ", false},
			},
			logql.LineFilter{
				Op: logql.OpRe,
				By: re("^(foo|bar)$"),
			},
		},

		// IP filter.
		{
			[]line{
				{"", false},
				{"foo", false},
				{"127.0.0.1", false},
				{"foo 192.168.1.1", true},
				// Mapped address.
				{"foo ::ffff:192.168.1.1", true},
				// Mapped address in a hex format.
				{fmt.Sprintf(`foo ::ffff:%02x%02x:%02x%02x`, 192, 168, 1, 1), true},
			},
			logql.LineFilter{
				Op: logql.OpEq,
				By: ip("192.168.1.0/24"),
			},
		},

		// OR filter.
		{
			[]line{
				{"", false},
				{"foo", true},
				{"bar", true},
				{" bar ", false},
			},
			logql.LineFilter{
				Op: logql.OpRe,
				By: re("^foo$"),
				Or: []logql.LineFilterValue{
					re("^bar$"),
				},
			},
		},
		{
			[]line{
				{"", false},
				{"foo", false},
				{"127.0.0.1", false},
				{"foo 192.168.1.1", true},
				{"foo 192.168.1.2", true},
				// Mapped address.
				{"foo ::ffff:192.168.1.1", true},
			},
			logql.LineFilter{
				Op: logql.OpEq,
				By: ip("192.168.1.1"),
				Or: []logql.LineFilterValue{
					ip("192.168.1.2"),
				},
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			set := logqlabels.NewLabelSet()

			t.Run("Filter", func(t *testing.T) {
				filter := tt.filter
				f, err := buildLineFilter(&filter)
				require.NoError(t, err)

				for _, input := range tt.inputs {
					newLine, gotOk := f.Process(0, input.line, set)
					// Ensure that extractor does not change the line.
					require.Equal(t, input.line, newLine)
					assert.Equal(t, input.keep, gotOk, "apply %#q to %#q", filter, input.line)
				}
			})

			t.Run("Negated", func(t *testing.T) {
				filter := tt.filter
				switch filter.Op {
				case logql.OpEq:
					filter.Op = logql.OpNotEq
				case logql.OpNotEq:
					filter.Op = logql.OpEq
				case logql.OpRe:
					filter.Op = logql.OpNotRe
				case logql.OpNotRe:
					filter.Op = logql.OpRe
				case logql.OpPattern:
					filter.Op = logql.OpNotPattern
				case logql.OpNotPattern:
					filter.Op = logql.OpPattern
				default:
					t.Fatalf("unexpected op %+v", filter.Op)
				}

				f, err := buildLineFilter(&filter)
				require.NoError(t, err)

				for _, input := range tt.inputs {
					newLine, gotOk := f.Process(0, input.line, set)
					// Ensure that extractor does not change the line.
					require.Equal(t, input.line, newLine)
					assert.Equal(t, !input.keep, gotOk, "apply %#q to %#q", filter, input.line)
				}
			})
		})
	}
}

func TestBuildLineFilter(t *testing.T) {
	for i, tt := range []struct {
		input   logql.LineFilter
		wantErr string
	}{
		{logql.LineFilter{}, `unexpected operation "<unknown op 0>"`},
		{logql.LineFilter{Op: logql.OpAdd}, `unexpected operation "+"`},
		{
			logql.LineFilter{
				Op: logql.OpEq,
				By: logql.LineFilterValue{IP: true, Value: "1"},
			},
			`invalid addr "1": ParseAddr("1"): unable to parse IP`,
		},
		{
			logql.LineFilter{
				Op: logql.OpRe,
				By: logql.LineFilterValue{IP: true, Value: "127.0.0.1"},
			},
			`unexpected operation "=~"`,
		},
		{
			logql.LineFilter{
				Op: logql.OpRe,
				By: logql.LineFilterValue{IP: true, Value: "127.0.0.1-127.0.0.2"},
			},
			`unexpected operation "=~"`,
		},
		{
			logql.LineFilter{
				Op: logql.OpRe,
				By: logql.LineFilterValue{IP: true, Value: "127.0.0.0/24"},
			},
			`unexpected operation "=~"`,
		},
		{
			logql.LineFilter{
				Op: logql.OpPattern,
				By: logql.LineFilterValue{Value: "<_> foo <_>"},
			},
			`|> line filter is unsupported`,
		},
	} {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			_, err := buildLineFilter(&tt.input)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

var ipLineFilterTests = []struct {
	input   string
	pattern string
	wantOk  bool
}{
	// Empty line.
	{
		``,
		"192.168.1.1",
		false,
	},
	// No IP in a line.
	{
		`foo`,
		"192.168.1.1",
		false,
	},
	{
		`foo`,
		"192.168.1.0-192.168.1.255",
		false,
	},
	{
		`foo`,
		"192.168.1.0/24",
		false,
	},

	// Has match.
	{
		`foo 192.168.1.1`,
		"192.168.1.1",
		true,
	},
	{
		`foo 192.168.1.1`,
		"192.168.1.0-192.168.1.255",
		true,
	},
	{
		`foo 192.168.1.1`,
		"192.168.1.0/24",
		true,
	},

	// No match.
	{
		`foo 127.0.0.1`,
		"192.168.1.1",
		false,
	},
	{
		`foo 127.0.0.1`,
		"192.168.1.0-192.168.1.255",
		false,
	},
	{
		`foo 127.0.0.1`,
		"192.168.1.0/24",
		false,
	},

	// Second IP match.
	{
		`foo=bar host=127.0.0.1 from=192.168.1.1`,
		"192.168.1.1",
		true,
	},

	// IPv6 match.
	{
		`::1`,
		"::1",
		true,
	},
	{
		`foo ::1 foo`,
		"::1",
		true,
	},
	{
		`2001:db8::68`,
		"2001:db8::68",
		true,
	},
	{
		`foo 2001:db8::68 foo`,
		"2001:db8::68",
		true,
	},
	// Mapped IPv4 match.
	{
		`foo ::ffff:192.168.1.1 foo`,
		"192.168.1.1",
		true,
	},
	{
		fmt.Sprintf(`foo ::ffff:%02x%02x:%02x%02x foo`, 192, 168, 1, 1),
		"192.168.1.1",
		true,
	},
}

func TestIPLineFilter(t *testing.T) {
	for i, tt := range ipLineFilterTests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			set := logqlabels.NewLabelSet()

			f, err := buildLineFilter(&logql.LineFilter{
				Op: logql.OpEq,
				By: logql.LineFilterValue{
					Value: tt.pattern,
					IP:    true,
				},
			})
			require.NoError(t, err)

			newLine, gotOk := f.Process(0, tt.input, set)
			// Ensure that extractor does not change the line.
			require.Equal(t, tt.input, newLine)
			require.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func FuzzIPLineFilter(f *testing.F) {
	for _, tt := range ipLineFilterTests {
		f.Add(tt.input, tt.pattern)
	}

	f.Fuzz(func(t *testing.T, line, pattern string) {
		f, err := buildLineFilter(&logql.LineFilter{
			Op: logql.OpEq,
			By: logql.LineFilterValue{
				Value: pattern,
				IP:    true,
			},
		})
		if err != nil {
			t.Skipf("Invalid pattern: %q", pattern)
		}

		// Ensure there is no crash.
		f.Process(1, line, logqlabels.LabelSet{})
	})
}

func Test_tryCaptureIPv4(t *testing.T) {
	tests := []struct {
		input   string
		capture string
	}{
		{`1.1.1.1`, `1.1.1.1`},
		{`10.1.1.1`, `10.1.1.1`},
		{`127.0.0.1`, `127.0.0.1`},
		{`127.0.0.1 foo`, `127.0.0.1`},

		{``, ``},
		{`1`, ``},
		{`foo`, ``},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, match := tryCaptureIPv4(tt.input)
			if tt.capture == "" {
				require.Empty(t, got)
				require.False(t, match)
				return
			}
			require.Equal(t, tt.capture, got)
			require.True(t, match)
		})
	}
}

func Test_tryCaptureIPv6(t *testing.T) {
	tests := []struct {
		input   string
		capture string
	}{
		{`::1`, `::1`},
		{`::1 foo`, `::1`},
		{`2001:db8::68`, `2001:db8::68`},
		{`2001:db8::68 foo`, `2001:db8::68`},

		{``, ``},
		{`:f`, ``},
		{`1`, ``},
		{`foo`, ``},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, match := tryCaptureIPv6(tt.input)
			if tt.capture == "" {
				require.Empty(t, got)
				require.False(t, match)
				return
			}
			require.Equal(t, tt.capture, got)
			require.True(t, match)
		})
	}
}
