package clusteradmin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFanoutPreservesProviderPeers(t *testing.T) {
	t.Parallel()
	peers := []Peer{{Node: "c"}, {Node: "a"}, {Node: "b"}}
	aggregator, err := New(Options{Peers: staticPeers{peers: peers}})
	require.NoError(t, err)
	answers, err := fanout(t.Context(), aggregator, "test", func(_ context.Context, p Peer) (string, error) { return p.Node, nil })
	require.NoError(t, err)
	assert.Equal(t, []Peer{{Node: "c"}, {Node: "a"}, {Node: "b"}}, peers, "fanout must not mutate a provider-owned slice")
	require.Len(t, answers, 3)
	for i, node := range []string{"a", "b", "c"} {
		assert.Equal(t, node, answers[i].Peer.Node)
		assert.Equal(t, node, answers[i].Value)
		assert.NoError(t, answers[i].Err)
	}
}
