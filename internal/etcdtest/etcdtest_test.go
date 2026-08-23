package etcdtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFreeAddrsDistinct(t *testing.T) {
	for _, n := range []int{1, 2, 8} {
		addrs, err := freeAddrs(n)
		require.NoError(t, err)
		require.Len(t, addrs, n)

		seen := make(map[string]struct{}, n)
		for _, addr := range addrs {
			require.NotContains(t, seen, addr)
			seen[addr] = struct{}{}
		}
	}
}

func TestStartServesClientEndpoint(t *testing.T) {
	require.NotEmpty(t, Start(t))
}
