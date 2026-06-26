package lexer

import (
	"fmt"
	"testing"
	"text/scanner"

	"github.com/stretchr/testify/require"
)

type TestCase struct {
	input   string
	want    []Token
	wantErr bool
}

var tests = []TestCase{
	{
		`process_cpu:cpu:nanoseconds:cpu:nanoseconds`,
		[]Token{
			{Type: Ident, Text: "process_cpu:cpu:nanoseconds:cpu:nanoseconds"},
		},
		false,
	},
	{
		`{service_name="frontend"}`,
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: Ident, Text: "service_name"},
			{Type: Eq, Text: "="},
			{Type: String, Text: "frontend"},
			{Type: CloseBrace, Text: "}"},
		},
		false,
	},
	{
		`{a="1",b!="2",c=~"3",d!~"4"}`,
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: Ident, Text: "a"},
			{Type: Eq, Text: "="},
			{Type: String, Text: "1"},
			{Type: Comma, Text: ","},
			{Type: Ident, Text: "b"},
			{Type: NotEq, Text: "!="},
			{Type: String, Text: "2"},
			{Type: Comma, Text: ","},
			{Type: Ident, Text: "c"},
			{Type: Re, Text: "=~"},
			{Type: String, Text: "3"},
			{Type: Comma, Text: ","},
			{Type: Ident, Text: "d"},
			{Type: NotRe, Text: "!~"},
			{Type: String, Text: "4"},
			{Type: CloseBrace, Text: "}"},
		},
		false,
	},
	{
		// Dotted OTel label name and a comment.
		"{service.name=\"x\"} # comment",
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: Ident, Text: "service.name"},
			{Type: Eq, Text: "="},
			{Type: String, Text: "x"},
			{Type: CloseBrace, Text: "}"},
		},
		false,
	},
	// Unterminated string.
	{`{a="x}`, nil, true},
}

func TestTokenize(t *testing.T) {
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := Tokenize(tt.input, TokenizeOptions{})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			for i := range got {
				// Zero position before checking.
				got[i].Pos = scanner.Position{}
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func FuzzTokenize(f *testing.F) {
	for _, tt := range tests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		_, _ = Tokenize(input, TokenizeOptions{})
	})
}
