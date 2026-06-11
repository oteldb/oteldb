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
		`3h`,
		[]Token{
			{Type: Duration, Text: "3h"},
		},
		false,
	},
	{
		`3h2m1.99s`,
		[]Token{
			{Type: Duration, Text: "3h2m1.99s"},
		},
		false,
	},
	{
		`3.5h`,
		[]Token{
			{Type: Duration, Text: "3.5h"},
		},
		false,
	},
	{
		`.5h`,
		[]Token{
			{Type: Duration, Text: ".5h"},
		},
		false,
	},
	{
		`10`,
		[]Token{
			{Type: Integer, Text: "10"},
		},
		false,
	},
	{
		`-10`,
		[]Token{
			{Type: Integer, Text: "-10"},
		},
		false,
	},
	{
		`10.5`,
		[]Token{
			{Type: Number, Text: "10.5"},
		},
		false,
	},
	{
		`-10.5`,
		[]Token{
			{Type: Number, Text: "-10.5"},
		},
		false,
	},
	{
		`.5`,
		[]Token{
			{Type: Number, Text: ".5"},
		},
		false,
	},
	{
		`parent`,
		[]Token{
			{Type: Parent, Text: "parent"},
		},
		false,
	},
	{
		`.foo.bar`,
		[]Token{
			{Type: Ident, Text: ".foo.bar"},
		},
		false,
	},
	{
		`span.foo.bar`,
		[]Token{
			{Type: Ident, Text: "span.foo.bar"},
		},
		false,
	},
	{
		`resource.foo.bar`,
		[]Token{
			{Type: Ident, Text: "resource.foo.bar"},
		},
		false,
	},
	{
		`parent.foo.bar`,
		[]Token{
			{Type: Ident, Text: "parent.foo.bar"},
		},
		false,
	},
	{
		`parent.span.foo.bar`,
		[]Token{
			{Type: Ident, Text: "parent.span.foo.bar"},
		},
		false,
	},
	{
		`parent.resource.foo.bar`,
		[]Token{
			{Type: Ident, Text: "parent.resource.foo.bar"},
		},
		false,
	},
	{
		`resource.github.com/ogen-go/ogen.attr`,
		[]Token{
			{Type: Ident, Text: "resource.github.com/ogen-go/ogen.attr"},
		},
		false,
	},
	{
		`{}`,
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: CloseBrace, Text: "}"},
		},
		false,
	},
	{
		`{ .a || .b } ~ { .c || .d }`,
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: Ident, Text: ".a"},
			{Type: Or, Text: "||"},
			{Type: Ident, Text: ".b"},
			{Type: CloseBrace, Text: "}"},

			{Type: Tilde, Text: "~"},

			{Type: OpenBrace, Text: "{"},
			{Type: Ident, Text: ".c"},
			{Type: Or, Text: "||"},
			{Type: Ident, Text: ".d"},
			{Type: CloseBrace, Text: "}"},
		},
		false,
	},
	{
		`{ .a } | by(.namespace) | coalesce() | avg(duration) = 1s`,
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: Ident, Text: ".a"},
			{Type: CloseBrace, Text: "}"},

			{Type: Pipe, Text: "|"},
			{Type: By, Text: "by"},
			{Type: OpenParen, Text: "("},
			{Type: Ident, Text: ".namespace"},
			{Type: CloseParen, Text: ")"},

			{Type: Pipe, Text: "|"},
			{Type: Coalesce, Text: "coalesce"},
			{Type: OpenParen, Text: "("},
			{Type: CloseParen, Text: ")"},

			{Type: Pipe, Text: "|"},
			{Type: Avg, Text: "avg"},
			{Type: OpenParen, Text: "("},
			{Type: SpanDuration, Text: "duration"},
			{Type: CloseParen, Text: ")"},
			{Type: Eq, Text: "="},
			{Type: Duration, Text: "1s"},
		},
		false,
	},

	{
		`
# foo

10`,
		[]Token{
			{Type: Integer, Text: "10"},
		},
		false,
	},
	{
		`
# foo
# bar
10`,
		[]Token{
			{Type: Integer, Text: "10"},
		},
		false,
	},
}

func TestTokenizeErrors(t *testing.T) {
	tests := []struct {
		input   string
		wantErr string
	}{
		{
			`10yy`,
			`at test.ql:1:1: unknown unit "yy" in duration "10yy"`,
		},
		{
			`{"foo"=~"\x"}`,
			`at test.ql:1:9: unquote string: invalid syntax`,
		},
		{
			`0ee1`,
			`at test.ql:1:1: exponent has no digits`,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			_, err := Tokenize(tt.input, TokenizeOptions{
				Filename: "test.ql",
			})
			require.EqualError(t, err, tt.wantErr)
		})
	}
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
		defer func() {
			if r := recover(); r != nil || t.Failed() {
				t.Logf("Input:\n%s", input)
			}

			_, _ = Tokenize(input, TokenizeOptions{})
		}()
	})
}
