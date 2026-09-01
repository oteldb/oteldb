package clusterquery_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/readbudget"

	"github.com/oteldb/oteldb/internal/clusterquery"
)

// The aggregator needs a bound of its own: it holds every shard owner's answer at once to merge
// them, so the owners' individual limits do not add up to one here.
func TestSourceQueryBudget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("sized from the configured limit", func(t *testing.T) {
		t.Parallel()

		b := readbudget.From(clusterquery.New(nil, 4096).WithQueryBudget(ctx))
		require.NotNil(t, b)
		assert.Equal(t, int64(4096), b.Remaining())
	})

	t.Run("negative leaves reads unbounded", func(t *testing.T) {
		t.Parallel()

		assert.Nil(t, readbudget.From(clusterquery.New(nil, -1).WithQueryBudget(ctx)))
	})

	// A nested install would hand the inner call a fresh allowance and uncap the query it was meant
	// to bound.
	t.Run("idempotent", func(t *testing.T) {
		t.Parallel()

		src := clusterquery.New(nil, 4096)
		outer := src.WithQueryBudget(ctx)

		assert.Equal(t, readbudget.From(outer), readbudget.From(src.WithQueryBudget(outer)))
	})
}
