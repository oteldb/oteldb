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
