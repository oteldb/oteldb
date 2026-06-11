package jsonexpr

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-faster/jx"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/logql"
)

func parseExprs(t require.TestingT, exprs ...string) map[logql.Label]Path {
	selectors := make(map[logql.Label]Path, len(exprs))
	for _, expr := range exprs {
		sel, err := Parse(expr)
		require.NoError(t, err)
		selectors[logql.Label(expr)] = sel
	}
	return selectors
}

func TestExtract(t *testing.T) {
	tests := []struct {
		input   string
		paths   map[logql.Label]Path
		want    map[logql.Label]string
		wantErr bool
	}{
		{
			`{
				"literal": "foo",
				"obj": {"c": "bar"},
				"arr": [{"e": "baz"}]
			}`,
			parseExprs(t,
				"literal",
				"obj",
				"obj.c",
				`obj["c"]`,
				"arr",
				"arr[0]",
				"arr[0].e",

				// Wrong type.
				"literal[0]",
				"literal.key",
				`literal["key"]`,
				"obj[0]",
				"arr.key",
			),
			map[logql.Label]string{
				"literal":  "foo",
				"obj":      `{"c": "bar"}`,
				"obj.c":    "bar",
				`obj["c"]`: "bar",
				"arr":      `[{"e": "baz"}]`,
				"arr[0]":   `{"e": "baz"}`,
				"arr[0].e": "baz",
			},
			false,
		},
		// Test different types.
		{
			`{
				"str": "foo\n",
				"integer": 10,
				"float": 3.14,
				"null": null,
				"bool": true
			}`,
			parseExprs(t,
				"str",
				"integer",
				"float",
				"null",
				"bool",
			),
			map[logql.Label]string{
				"str":     "foo\n",
				"integer": "10",
				"float":   "3.14",
				"null":    "",
				"bool":    "true",
			},
			false,
		},
		{
			`{
				"a": {"b": "correct"},
				"a.b": "wrong"
			}`,
			parseExprs(t,
				"a.b",
			),
			map[logql.Label]string{
				"a.b": "correct",
			},
			false,
		},
		{
			`{
				"a": ["correct", "wrong"],
				"a.0": "wrong",
				"a[0]": "wrong"
			}`,
			parseExprs(t,
				"a[0]",
			),
			map[logql.Label]string{
				"a[0]": "correct",
			},
			false,
		},

		// Invalid JSON.
		{
			`{`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": }`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": {,}}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": [,]}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": "foo\"}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`["foo\"]`,
			map[logql.Label]Path{"a": {IndexSel(0)}},
			nil,
			true,
		},
		{
			`{"a": 1ee1}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": nul}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": tru}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			d := jx.DecodeStr(tt.input)
			got := make(map[logql.Label]string, len(tt.paths))

			err := Extract(
				d,
				MakeSelectorTree(tt.paths),
				func(l logql.Label, s string) {
					got[l] = s
				},
			)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func FuzzExtract(f *testing.F) {
	for _, tt := range []struct {
		input string
		path  string
	}{
		{`{"foo": "bar"}`, `["foo"]`},
		{`{"foo": "bar"}`, `foo"`},
		{`[{"foo": "bar"}]`, `[0].foo"`},
	} {
		f.Add([]byte(tt.input), tt.path)
	}

	f.Fuzz(func(t *testing.T, input []byte, expr string) {
		if !json.Valid(input) {
			t.Skipf("Invalid JSON: %q", input)
			return
		}

		sel, err := Parse(expr)
		if err != nil {
			t.Skipf("Invalid path: %q", expr)
			return
		}

		_ = Extract(
			jx.DecodeBytes(input),
			MakeSelectorTree(map[logql.Label]Path{
				"fuzz": sel,
			}),
			func(l logql.Label, s string) {},
		)
	})
}
