package clusteradmin

import (
	"context"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// GetRuntime implements getRuntime operation.
//
// Every figure is the sum over the nodes that answered, which is what a cluster-wide heap or CPU
// budget is. Nothing here is deduplicated — a Go runtime counter belongs to one process — so a
// missing node subtracts its share and the total silently shrinks; /api/v1/health is where an
// absent node is visible.
func (a *Aggregator) GetRuntime(ctx context.Context) (*adminapi.RuntimeStats, error) {
	answers, err := fanout(ctx, a, "runtime",
		func(ctx context.Context, p Peer) (*adminapi.RuntimeStats, error) { return p.Client.GetRuntime(ctx) },
	)
	if err != nil {
		return nil, err
	}

	var (
		stats    adminapi.RuntimeStats
		memLimit int64
		limited  int
		answered int
	)

	for _, r := range answers {
		if !r.ok() {
			continue
		}
		answered++

		v := r.Value
		stats.Goroutines += v.Goroutines
		stats.NumCPU += v.NumCPU
		stats.Gomaxprocs += v.Gomaxprocs
		stats.HeapAllocBytes += v.HeapAllocBytes
		stats.HeapInuseBytes += v.HeapInuseBytes
		stats.HeapSysBytes += v.HeapSysBytes
		stats.StackInuseBytes += v.StackInuseBytes
		stats.GcCount += v.GcCount
		stats.NextGcBytes += v.NextGcBytes

		if l, ok := v.MemLimitBytes.Get(); ok {
			memLimit += l
			limited++
		}
	}

	// A partial sum of memory limits is not a cluster memory limit: it would read as headroom that
	// the unlimited nodes do not have.
	if answered > 0 && limited == answered {
		stats.MemLimitBytes = adminapi.NewOptInt64(memLimit)
	}

	return &stats, nil
}
