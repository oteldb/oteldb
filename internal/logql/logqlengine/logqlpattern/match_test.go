package logqlpattern

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var matchTests = []struct {
	pattern string
	input   string
	match   map[string]string
	full    bool
	flags   ParseFlags
}{
	// Empty pattern matches empty input.
	{
		"",
		``,
		map[string]string{},
		true,
		LineFilterFlags,
	},
	{
		"",
		`f`,
		map[string]string{},
		false,
		LineFilterFlags,
	},

	{
		"status:<status>",
		`status:200`,
		map[string]string{
			"status": "200",
		},
		true,
		ExtractorFlags,
	},
	{
		"status:<status>",
		``,
		map[string]string{},
		false,
		ExtractorFlags,
	},
	{
		"<prefix>:<_>",
		`abc`,
		map[string]string{
			"prefix": "abc",
		},
		false,
		ExtractorFlags,
	},
	{
		"<method> <path>",
		`GET /foo`,
		map[string]string{
			"method": "GET",
			"path":   "/foo",
		},
		true,
		ExtractorFlags,
	},
	{
		"<_> bar",
		`foo bar baz`,
		map[string]string{},
		false,
		0,
	},
	{
		"foo <_>",
		`foo bar baz`,
		map[string]string{},
		true,
		0,
	},
	{
		"<_> baz",
		`foo bar baz`,
		map[string]string{},
		true,
		0,
	},
	{
		"<foo>",
		` bar `,
		map[string]string{
			"foo": ` bar `,
		},
		true,
		ExtractorFlags,
	},
	{
		"<_> bar <_>",
		` bar `,
		map[string]string{},
		false,
		0,
	},
	{
		"<_>bar<_>",
		` bar `,
		map[string]string{},
		true,
		0,
	},

	// Realistic patterns.
	{
		`<ip> - <user> [<_>] "<method> <path> <_>" <status> <size> <user_agent> <_>`,
		`127.0.0.1 - - [01/Jan/2000:00:00:00 +0000] "GET /foo HTTP/1.1" 200 1337 "UserAgent" "13.76.247.102, 34.120.177.193" "TLSv1.2" "US" ""`,
		map[string]string{
			"ip":         "127.0.0.1",
			"user":       "-",
			"method":     "GET",
			"path":       "/foo",
			"status":     "200",
			"size":       "1337",
			"user_agent": `"UserAgent"`,
		},
		true,
		ExtractorFlags,
	},
}

func TestMatch(t *testing.T) {
	for i, tt := range matchTests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil || t.Failed() {
					t.Logf("Pattern: %q", tt.pattern)
					t.Logf("Input: %#q", tt.input)
				}
			}()

			compiled, err := Parse(tt.pattern, tt.flags)
			require.NoError(t, err)

			matches := map[string]string{}
			fullMatch := Match(compiled, tt.input, func(label, value string) {
				matches[label] = value
			})
			require.Equal(t, tt.match, matches)
			require.Equal(t, tt.full, fullMatch)
		})
	}
}

func FuzzMatch(f *testing.F) {
	for _, tt := range matchTests {
		f.Add(tt.pattern, tt.input)
	}
	f.Fuzz(func(t *testing.T, pattern, input string) {
		compiled, err := Parse(pattern, ExtractorFlags)
		if err != nil {
			t.Skipf("Invalid pattern %q: %+v", pattern, err)
			return
		}
		Match(compiled, input, func(string, string) {})
	})
}
