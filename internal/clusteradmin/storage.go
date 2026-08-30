package clusteradmin

import (
	"context"
	"sort"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// GetStorage implements getStorage operation.
//
// Every counter is the sum over the nodes that answered, so replicated data is counted once per
// replica — these are per-node schemas and there is nothing in them to deduplicate by.
// /api/v1/cluster/storage is the view that separates the two readings.
func (a *Aggregator) GetStorage(ctx context.Context) (*adminapi.StorageStats, error) {
	answers, err := fanout(ctx, a, "storage",
		func(ctx context.Context, p Peer) (*adminapi.StorageStats, error) { return p.Client.GetStorage(ctx) },
	)
	if err != nil {
		return nil, err
	}

	stats := &adminapi.StorageStats{}
	engine := adminapi.EngineStats{Tenants: []adminapi.TenantStats{}}

	tenants := map[string]*adminapi.TenantStats{}
	var order []string
	var haveEngine bool

	for _, r := range answers {
		if !r.ok() {
			continue
		}

		stats.StorageEnabled = stats.StorageEnabled || r.Value.StorageEnabled
		stats.ClickhouseEnabled = stats.ClickhouseEnabled || r.Value.ClickhouseEnabled

		e, ok := r.Value.Engine.Get()
		if !ok {
			continue
		}
		haveEngine = true

		addCaches(&engine.Caches, e.Caches)
		addMaintenance(&engine.Maintenance, e.Maintenance)

		if c, ok := e.Cluster.Get(); ok {
			mergeClusterStats(&engine.Cluster, c)
		}

		for _, t := range e.Tenants {
			into, seen := tenants[t.Tenant]
			if !seen {
				into = &adminapi.TenantStats{Tenant: t.Tenant, Signals: []adminapi.EngineSignalStats{}}
				tenants[t.Tenant] = into
				order = append(order, t.Tenant)
			}
			addTenantStats(into, t)
		}
	}

	if !haveEngine {
		return stats, nil
	}

	sort.Strings(order)
	for _, name := range order {
		engine.Tenants = append(engine.Tenants, *tenants[name])
	}
	stats.Engine = adminapi.NewOptEngineStats(engine)

	return stats, nil
}

func addCaches(into *adminapi.CacheStats, from adminapi.CacheStats) {
	into.DecodeCache.Hits += from.DecodeCache.Hits
	into.DecodeCache.Misses += from.DecodeCache.Misses
	into.DecodeCache.Bytes += from.DecodeCache.Bytes
	into.DecodeCache.Items += from.DecodeCache.Items
}

// addMaintenance sums the cycle counters and keeps the most recent cycle, which is the one an
// operator is asking about when the page says a merge just ran.
func addMaintenance(into *adminapi.MaintenanceStats, from adminapi.MaintenanceStats) {
	into.Cycles += from.Cycles
	into.LastCycleTasks += from.LastCycleTasks

	start, ok := from.LastCycleStart.Get()
	if !ok {
		return
	}
	if cur, had := into.LastCycleStart.Get(); had && !start.After(cur) {
		return
	}

	into.LastCycleStart = adminapi.NewOptDateTime(start)
	into.LastCycleDurationSeconds = from.LastCycleDurationSeconds
}

// mergeClusterStats folds a node's cluster view into the aggregate. Membership is a cluster-level
// fact and is taken from the first node that reports it; the part-sync and erasure-coding counters
// are per-node tallies of cluster work and are summed. Self stays empty: the aggregator is not a
// ring member, and naming one node there would read as the cluster having a single identity.
func mergeClusterStats(into *adminapi.OptClusterStats, from adminapi.ClusterStats) {
	cur, ok := into.Get()
	if !ok {
		cur = adminapi.ClusterStats{Members: from.Members, Owned: []string{}}
	}

	owned := map[string]struct{}{}
	for _, s := range cur.Owned {
		owned[s] = struct{}{}
	}
	for _, s := range from.Owned {
		owned[s] = struct{}{}
	}

	cur.Owned = cur.Owned[:0]
	for s := range owned {
		cur.Owned = append(cur.Owned, s)
	}
	sort.Strings(cur.Owned)

	if ps, ok := from.PartSync.Get(); ok {
		acc, _ := cur.PartSync.Get()
		acc.Passes += ps.Passes
		acc.Mirrored += ps.Mirrored
		acc.Copied += ps.Copied
		acc.CopiedBytes += ps.CopiedBytes
		acc.Pruned += ps.Pruned
		acc.Errors += ps.Errors
		if last, ok := ps.LastSync.Get(); ok {
			if prev, had := acc.LastSync.Get(); !had || last.After(prev) {
				acc.LastSync = adminapi.NewOptDateTime(last)
			}
		}
		cur.PartSync = adminapi.NewOptPartSyncStats(acc)
	}

	if ec, ok := from.Ec.Get(); ok {
		acc, _ := cur.Ec.Get()
		acc.Converted += ec.Converted
		acc.ConvertErrors += ec.ConvertErrors
		acc.RepairedSlots += ec.RepairedSlots
		acc.RepairErrors += ec.RepairErrors
		acc.PrunedStagedParts += ec.PrunedStagedParts
		acc.Reconstructs += ec.Reconstructs
		acc.ReconstructErrors += ec.ReconstructErrors
		cur.Ec = adminapi.NewOptECStats(acc)
	}

	*into = adminapi.NewOptClusterStats(cur)
}

func addTenantStats(into *adminapi.TenantStats, from adminapi.TenantStats) {
	into.TotalSeries += from.TotalSeries
	into.TotalParts += from.TotalParts

	into.Admission.Accepted += from.Admission.Accepted
	into.Admission.RejectedOoo += from.Admission.RejectedOoo
	into.Admission.RejectedRate += from.Admission.RejectedRate
	into.Admission.RejectedCardinality += from.Admission.RejectedCardinality
	into.Admission.RejectedInFlight += from.Admission.RejectedInFlight
	into.Admission.SampledDropped += from.Admission.SampledDropped
	into.Admission.Overflowed += from.Admission.Overflowed

	for _, s := range from.Signals {
		i := sort.Search(len(into.Signals), func(i int) bool { return into.Signals[i].Signal >= s.Signal })
		if i < len(into.Signals) && into.Signals[i].Signal == s.Signal {
			addSignalStats(&into.Signals[i], s)

			continue
		}
		into.Signals = append(into.Signals, adminapi.EngineSignalStats{})
		copy(into.Signals[i+1:], into.Signals[i:])
		into.Signals[i] = s
	}
}

func addSignalStats(into *adminapi.EngineSignalStats, from adminapi.EngineSignalStats) {
	into.Series += from.Series
	into.HeadItems += from.HeadItems
	into.HeadBytes += from.HeadBytes
	into.Parts += from.Parts
	into.SealedParts += from.SealedParts
	into.MergeBacklog += from.MergeBacklog
	into.MergeCandidates += from.MergeCandidates
	into.WalSegments += from.WalSegments
	into.WalBytes += from.WalBytes
	into.MergeRunning = into.MergeRunning || from.MergeRunning
	into.Wal = into.Wal || from.Wal

	if from.MergeCapBytes > into.MergeCapBytes {
		into.MergeCapBytes = from.MergeCapBytes
	}
	if t, ok := from.MinTime.Get(); ok {
		if cur, had := into.MinTime.Get(); !had || t.Before(cur) {
			into.MinTime = adminapi.NewOptDateTime(t)
		}
	}
	if t, ok := from.MaxTime.Get(); ok {
		if cur, had := into.MaxTime.Get(); !had || t.After(cur) {
			into.MaxTime = adminapi.NewOptDateTime(t)
		}
	}
}
