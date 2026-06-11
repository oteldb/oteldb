package faker

import (
	"net/netip"
	"testing"
)

func TestIPAllocator_Next(t *testing.T) {
	allocator := newIPAllocator(netip.MustParseAddr("10.0.5.0"))
	seen := make(map[netip.Addr]struct{})
	for range 1600 {
		ip := allocator.Next()
		if _, ok := seen[ip]; ok {
			t.Fatalf("duplicate ip address: %s", ip)
		}
		seen[ip] = struct{}{}
	}
	t.Logf("%s", allocator.Next())
}
