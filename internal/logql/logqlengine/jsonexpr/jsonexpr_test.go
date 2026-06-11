package jsonexpr

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var parseTests = []struct {
	input   string
	want    Path
	wantErr bool
}{
	{`[0]`, Path{IndexSel(0)}, false},
	{`[0][10]`, Path{IndexSel(0), IndexSel(10)}, false},
	{`foo`, Path{KeySel("foo")}, false},
	{
		`foo.bar.baz`,
		Path{
			KeySel("foo"),
			KeySel("bar"),
			KeySel("baz"),
		},
		false,
	},
	{
		`["foo"]["bar"]["baz"]`,
		Path{
			KeySel("foo"),
			KeySel("bar"),
			KeySel("baz"),
		},
		false,
	},
	{
		`foo.bar["baz"]`,
		Path{
			KeySel("foo"),
			KeySel("bar"),
			KeySel("baz"),
		},
		false,
	},
	{
		`foo["bar"]["baz"]`,
		Path{
			KeySel("foo"),
			KeySel("bar"),
			KeySel("baz"),
		},
		false,
	},
	{
		`["foo"].bar["baz"]`,
		Path{
			KeySel("foo"),
			KeySel("bar"),
			KeySel("baz"),
		},
		false,
	},
	{
		`["foo"]["bar"].baz`,
		Path{
			KeySel("foo"),
			KeySel("bar"),
			KeySel("baz"),
		},
		false,
	},

	{``, nil, true},
	{`0`, nil, true},
	{`"foo"`, nil, true},
	{`[`, nil, true},
	{`[]`, nil, true},
	{`["]`, nil, true},
	{`["\`, nil, true},
	{`["\]`, nil, true},
	{`["\"]`, nil, true},
	{`["\n]`, nil, true},
	{`["foo"."]`, nil, true},
	{`["\xxx"]`, nil, true},
	{`["foo"`, nil, true},
	{`foo.0baz`, nil, true},
}

func TestParse(t *testing.T) {
	for i, tt := range parseTests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := Parse(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func FuzzParse(f *testing.F) {
	for _, tt := range parseTests {
		f.Add(tt.input)
	}

	f.Fuzz(func(t *testing.T, input string) {
		_, _ = Parse(input)
	})
}
