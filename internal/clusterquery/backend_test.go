package clusterquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sigmetric "github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/clusterquery"
	"github.com/oteldb/oteldb/internal/etcdtest"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// TestBackendServesFromCluster is the whole premise of the stateless node: the query stack built
// over a routed cluster view must answer the same way it does over a local engine, without any of
// it knowing the data is remote.
func TestBackendServesFromCluster(t *testing.T) {
	t.Parallel()

	const shards = 2

	endpoint := etcdtest.Start(t)

	keys := shardKeys(shards)
	startNode(t, endpoint, "node-a", map[string][]string{
		keys[0]: {"up"},
		keys[1]: {"http_requests_total"},
	})

	b := storagebackend.NewQuery(clusterquery.New(openRouter(t, endpoint, 1, shards), 0))

	q, err := b.Querier(0, 1000)
	require.NoError(t, err)

	t.Cleanup(func() { _ = q.Close() })

	values, _, err := q.LabelValues(t.Context(), "__name__", nil)
	require.NoError(t, err)

	// Both shards' series, reached through the label endpoint's enumeration seam.
	assert.Equal(t, []string{"http_requests_total", "up"}, values)
}

// TestBackendRefusesIngest pins that a query-only backend says so rather than panicking on the
// engine it does not have.
func TestBackendRefusesIngest(t *testing.T) {
	t.Parallel()

	b := storagebackend.NewQuery(clusterquery.New(nil, 0))

	require.ErrorIs(t, b.WriteMetrics(t.Context(), sigmetric.Metrics{}), storagebackend.ErrNoEngine)
	require.ErrorIs(t, b.MaintainNow(t.Context()), storagebackend.ErrNoEngine)
	assert.Empty(t, b.Inspect().Tenants)
}
