package logqlpattern

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var parseTests = []struct {
	input   string
	wantP   []Part
	wantErr string
	flags   ParseFlags
}{
	{
		"<foo>",
		[]Part{capture("foo")},
		"",
		ExtractorFlags,
	},
	{
		"<_>",
		[]Part{capture("_")},
		"",
		0,
	},
	{
		"<_1foo>",
		[]Part{capture("_1foo")},
		"",
		ExtractorFlags,
	},
	{
		"<1> <foo>",
		[]Part{
			literal("<1> "),
			capture("foo"),
		},
		"",
		ExtractorFlags,
	},
	{
		"<foo ><foo>",
		[]Part{
			literal("<foo >"),
			capture("foo"),
		},
		"",
		ExtractorFlags,
	},
	{
		"<foo>|<bar>",
		[]Part{
			capture("foo"),
			literal("|"),
			capture("bar"),
		},
		"",
		ExtractorFlags,
	},
	{
		"> <method> <",
		[]Part{
			literal("> "),
			capture("method"),
			literal(" <"),
		},
		"",
		ExtractorFlags,
	},
	{
		"_> <method> <_",
		[]Part{
			literal("_> "),
			capture("method"),
			literal(" "),
			literal("<_"),
		},
		"",
		ExtractorFlags,
	},
	{
		"status:<status>",
		[]Part{
			literal("status:"),
			capture("status"),
		},
		"",
		ExtractorFlags,
	},
	{
		"<method> - status:<status>",
		[]Part{
			capture("method"),
			literal(" - status:"),
			capture("status"),
		},
		"",
		ExtractorFlags,
	},
	{
		`<ip> - <user> [<_>] "<method> <path> <_>" <status> <size> <url> <user_agent>`,
		[]Part{
			capture("ip"),
			literal(" - "),
			capture("user"),
			literal(" ["),
			capture("_"),
			literal(`] "`),
			capture("method"),
			literal(" "),
			capture("path"),
			literal(" "),
			capture("_"),
			literal(`" `),
			capture("status"),
			literal(" "),
			capture("size"),
			literal(" "),
			capture("url"),
			literal(" "),
			capture("user_agent"),
		},
		"",
		ExtractorFlags,
	},

	{
		"",
		nil,
		"",
		LineFilterFlags,
	},

	{
		"\xff",
		nil,
		"pattern is invalid UTF-8",
		RequireCapture,
	},
	{
		" ",
		nil,
		"at least one capture is expected",
		RequireCapture,
	},
	{
		"status:",
		nil,
		"at least one capture is expected",
		RequireCapture,
	},
	{
		"<foo>|<foo>",
		nil,
		`duplicate capture "foo"`,
		ExtractorFlags,
	},
	{
		"<foo><bar>",
		nil,
		"consecutive capture: literal expected between <foo> and <bar>",
		0,
	},
	{
		"<foo>|<bar>",
		nil,
		`unexpected named pattern "foo"`,
		DisallowNamed,
	},
}

func capture(s string) Part {
	return Part{
		Type:  Capture,
		Value: s,
	}
}

func literal(s string) Part {
	return Part{
		Type:  Literal,
		Value: s,
	}
}

func TestParse(t *testing.T) {
	for i, tt := range parseTests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if r := recover(); t.Failed() || r != nil {
					t.Logf("Input: %q", tt.input)
				}
			}()

			gotP, err := Parse(tt.input, tt.flags)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				require.PanicsWithError(t, tt.wantErr, func() {
					MustParse(tt.input, tt.flags)
				})
				return
			}
			require.NoError(t, err)
			require.NotPanics(t, func() {
				MustParse(tt.input, tt.flags)
			})
			require.Equal(t, tt.wantP, gotP.Parts)
		})
	}
}

func FuzzParse(f *testing.F) {
	for _, tt := range parseTests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		defer func() {
			if r := recover(); t.Failed() || r != nil {
				t.Logf("Input: %q", input)
			}
		}()

		Parse(input, ExtractorFlags)
	})
}
