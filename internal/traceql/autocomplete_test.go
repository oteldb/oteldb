package traceql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var autocompleteTests = []struct {
	input string
	want  Autocomplete
}{
	{``, Autocomplete{}},
	{`{}`, Autocomplete{}},
	{`{ .a = }`, Autocomplete{}},
	{`{ .a = `, Autocomplete{}},
	// Contains OR operation.
	{`{ .a = 10 && .b = 20 || .c = 30 }`, Autocomplete{}},
	{`{ .a = 10 && .b =  || .c = 30 }`, Autocomplete{}},
	// Complicated sub-expression.
	{`{ .status = 2*100 && .baz = 10 }`, Autocomplete{}},
	{`{ .status / 100 = 2 && .baz = 10 }`, Autocomplete{}},
	// Invalid string.
	{`{ name =~ "\" }`, Autocomplete{}},
	// Invalid regex.
	{`{ name =~ 10 }`, Autocomplete{}},
	{`{ name =~ "\\" }`, Autocomplete{}},

	// Simple cases.
	{
		`{ .a = 10 }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 10},
				},
			},
		},
	},
	{
		`{ .a > 10 }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpGt,
					Static:    Static{Type: TypeInt, Data: 10},
				},
			},
		},
	},
	{
		`{ 10 > .a }`, // -> { .a < 10 }
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpLt,
					Static:    Static{Type: TypeInt, Data: 10},
				},
			},
		},
	},
	{
		`{ .a = 10 && .b =~ "foo.+" }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 10},
				},
				{
					Attribute: Attribute{Name: "b"},
					Op:        OpRe,
					Static:    Static{Type: TypeString, Str: "foo.+"},
				},
			},
		},
	},
	// Missing brace.
	{
		`{ .a = 10 `,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 10},
				},
			},
		},
	},
	// Missing sub-expression.
	{
		`{ .a = && .b = 20 && .c = 30 }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "b"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 20},
				},
				{
					Attribute: Attribute{Name: "c"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 30},
				},
			},
		},
	},
	{
		`{ .a = 10 && .b = && .c = 30 }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 10},
				},
				{
					Attribute: Attribute{Name: "c"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 30},
				},
			},
		},
	},
	{
		`{ .a = 10 && .b = 20 && .c = }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 10},
				},
				{
					Attribute: Attribute{Name: "b"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 20},
				},
			},
		},
	},
	{
		`{ .a = && .b = && .c = 30 }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "c"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 30},
				},
			},
		},
	},
}

func TestParseAutocomplete(t *testing.T) {
	for i, tt := range autocompleteTests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if t.Failed() {
					t.Logf("Input: %#q", tt.input)
				}
			}()

			got := ParseAutocomplete(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func FuzzParseAutocomplete(f *testing.F) {
	for _, tt := range autocompleteTests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		defer func() {
			if t.Failed() {
				t.Logf("Input: %#q", input)
			}
		}()

		ParseAutocomplete(input)
	})
}

func TestAutocompleteString(t *testing.T) {
	tests := []struct {
		input Autocomplete
		want  string
	}{
		{Autocomplete{}, `{}`},
		{
			Autocomplete{
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpLt,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
			`{ .a < 10 }`,
		},
		{
			Autocomplete{
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "b"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 20},
					},
					{
						Attribute: Attribute{Name: "c"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 30},
					},
				},
			},
			`{ .b = 20 && .c = 30 }`,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got := tt.input.String()
			require.Equal(t, tt.want, got)

			// Ensure that String generates valid TraceQL.
			_, err := Parse(got)
			require.NoError(t, err)

			// Ensure that String generates same autocomplete query.
			gotParsed := ParseAutocomplete(got)
			require.Equal(t, tt.input, gotParsed)
		})
	}
}
