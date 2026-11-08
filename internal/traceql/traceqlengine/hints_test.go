package traceqlengine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/traceql"
)

func TestBuildExprRejectsHints(t *testing.T) {
	t.Parallel()
	for _, query := range []string{
		`{} with(most_recent=true)`,
		`{} with(most_recent=false)`,
		`{} | rate() with(sample=true)`,
		`{} | compare({status=error}) with(exemplars=10)`,
		`{} with(unknown=1)`,
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()
			expr, err := traceql.Parse(query)
			require.NoError(t, err)
			_, err = BuildExpr(expr)
			var unsupported *UnsupportedError
			require.ErrorAs(t, err, &unsupported)
		})
	}
}
