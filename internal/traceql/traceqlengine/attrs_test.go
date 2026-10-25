package traceqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/traceql"
)

func TestReferencedAttributes(t *testing.T) {
	tests := []struct {
		query string
		want  []string
	}{
		{`{}`, nil},
		{`{ name = "Span #1" }`, nil},
		{`{ .service.name = "openrouter" }`, []string{"service.name"}},
		{`{ span.http.status_code = 200 }`, []string{"http.status_code"}},
		{
			`{ span.http.status_code = 200 && resource.service.name = "foo" }`,
			[]string{"http.status_code", "service.name"},
		},
		{
			`{ span.a = "x" } && { span.b = "y" }`,
			[]string{"a", "b"},
		},
		{
			`{ span.a = "x" } | avg(span.b) > 1`,
			[]string{"a", "b"},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			expr, err := traceql.Parse(tt.query)
			require.NoError(t, err)

			got := referencedAttributes(expr)
			gotNames := make([]string, 0, len(got))
			for name := range got {
				gotNames = append(gotNames, name)
			}

			require.ElementsMatch(t, tt.want, gotNames)
		})
	}
}

func TestReferencedFields(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		query  string
		fields []string
	}{
		{name: "empty", query: `{}`},
		{name: "intrinsic", query: `{status = error}`, fields: []string{"status"}},
		{name: "unary", query: `{!(event:name = "exception")}`, fields: []string{"event:name"}},
		{name: "binary", query: `{event:name = "exception"} && {link:spanID != nil}`, fields: []string{"event:name", "link:spanID"}},
		{name: "scalar", query: `{} | avg(span.cost) + max(duration) > 1s`, fields: []string{"span.cost", "duration"}},
		{name: "group", query: `{} | by(resource.service.name)`, fields: []string{"resource.service.name"}},
		{name: "select", query: `{} | select(event.tag, link.tag)`, fields: []string{"event.tag", "link.tag"}},
		{name: "duplicate", query: `{span.cost > 1 && span.cost < 3}`, fields: []string{"span.cost"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := traceql.Parse(tt.query)
			require.NoError(t, err)
			got := referencedFields(expr)
			require.NotNil(t, got, "an empty query still requests a projection")
			want := make([]traceql.Attribute, 0, len(tt.fields))
			for _, field := range tt.fields {
				attr, err := traceql.ParseAttribute(field)
				require.NoError(t, err)
				want = append(want, attr)
			}
			require.Equal(t, want, got)
		})
	}
}
