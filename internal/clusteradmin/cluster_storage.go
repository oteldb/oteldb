package clusteradmin

import (
	"context"
	"sort"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// GetClusterStorage implements getClusterStorage operation.
//
// It reports the cluster's footprint twice. The physical figures sum what every node holds, which
// counts a replicated part once per replica — 282 MB of disk for 141 MB of data at rf 2. The logical
// figures deduplicate by part id: a part keeps its backend key prefix when it is mirrored, so the
// same id on two nodes is one part.
//
// The dedupe is deliberately not "physical / rf". That identity holds only when replication is
// exactly met everywhere, and it is precisely when it is not — a rebalance in flight, a replica
// missing — that someone opens this page.
func (a *Aggregator) GetClusterStorage(ctx context.Context) (*adminapi.ClusterStorage, error) {
	answers, err := fanout(ctx, a, "storage/efficiency",
		func(ctx context.Context, p Peer) (*adminapi.EfficiencyStats, error) {
			return p.Client.GetEfficiency(ctx, adminapi.GetEfficiencyParams{Parts: adminapi.NewOptBool(true)})
		},
	)
	if err != nil {
		return nil, err
	}

	agg := newStorageAggregate(a.opts.ReplicationFactor)
	for _, r := range answers {
		if !r.ok() {
			agg.complete = false

			continue
		}
		agg.add(r.Peer.Node, r.Value)
	}

	return &adminapi.ClusterStorage{
		ReplicationFactor: a.opts.ReplicationFactor,
		Complete:          agg.complete,
		Nodes:             nodeStatuses(answers),
		Tenants:           agg.tenants(),
	}, nil
}

// signalKey identifies one (tenant, signal) across the cluster.
type signalKey struct {
	tenant string
	signal adminapi.Signal
}

// signalAggregate folds one (tenant, signal)'s per-node reports together.
type signalAggregate struct {
	// parts is the union of part ids across nodes, holding the size the first node to report each
	// part gave. Replicas of a part are byte-identical objects, so which node is asked does not
	// change the figure; taking the first keeps the union a set rather than a max.
	parts  map[string]partSize
	nodes  []adminapi.ClusterNodeSignalStorage
	stored int64
	points int64
	count  int64
}

// partSize is one distinct part's contribution to the deduplicated totals.
type partSize struct {
	bytes int64
	rows  int64
}

// storageAggregate accumulates a fan-out of efficiency reports into the cluster view.
type storageAggregate struct {
	rf       int
	complete bool
	byKey    map[signalKey]*signalAggregate
	order    []signalKey
}

func newStorageAggregate(rf int) *storageAggregate {
	return &storageAggregate{rf: rf, complete: true, byKey: map[signalKey]*signalAggregate{}}
}

// add folds one node's report in. A node that reports parts without their identities cannot be
// deduplicated — it still counts physically, but the report is no longer complete, because a part
// only that node holds is now invisible to the union.
func (s *storageAggregate) add(node string, stats *adminapi.EfficiencyStats) {
	for _, t := range stats.Tenants {
		for _, sig := range t.Signals {
			agg := s.lookup(signalKey{tenant: t.Tenant, signal: sig.Signal})

			agg.nodes = append(agg.nodes, adminapi.ClusterNodeSignalStorage{
				Node:   node,
				Bytes:  sig.StoredBytes,
				Parts:  sig.Parts,
				Points: sig.Points,
			})
			agg.stored += sig.StoredBytes
			agg.points += sig.Points
			agg.count += sig.Parts

			if len(sig.PartsDetail) == 0 && sig.Parts > 0 {
				s.complete = false

				continue
			}

			for _, p := range sig.PartsDetail {
				if _, seen := agg.parts[p.ID]; seen {
					continue
				}
				agg.parts[p.ID] = partSize{bytes: p.Bytes, rows: p.Rows}
			}
		}
	}
}

func (s *storageAggregate) lookup(k signalKey) *signalAggregate {
	agg, ok := s.byKey[k]
	if !ok {
		agg = &signalAggregate{parts: map[string]partSize{}}
		s.byKey[k] = agg
		s.order = append(s.order, k)
	}

	return agg
}

// tenants renders the accumulated state, sorted by tenant then signal so a dashboard polling it
// does not see rows move between refreshes.
func (s *storageAggregate) tenants() []adminapi.ClusterTenantStorage {
	sort.Slice(s.order, func(i, j int) bool {
		if s.order[i].tenant != s.order[j].tenant {
			return s.order[i].tenant < s.order[j].tenant
		}

		return s.order[i].signal < s.order[j].signal
	})

	out := []adminapi.ClusterTenantStorage{}
	for _, k := range s.order {
		agg := s.byKey[k]

		var logicalBytes, logicalPoints int64
		for _, p := range agg.parts {
			logicalBytes += p.bytes
			logicalPoints += p.rows
		}

		sig := adminapi.ClusterSignalStorage{
			Signal:            k.signal,
			LogicalBytes:      logicalBytes,
			PhysicalBytes:     agg.stored,
			LogicalParts:      int64(len(agg.parts)),
			PhysicalParts:     agg.count,
			LogicalPoints:     logicalPoints,
			PhysicalPoints:    agg.points,
			ReplicationFactor: s.rf,
			Nodes:             agg.nodes,
		}

		if n := len(out); n > 0 && out[n-1].Tenant == k.tenant {
			out[n-1].Signals = append(out[n-1].Signals, sig)

			continue
		}

		out = append(out, adminapi.ClusterTenantStorage{
			Tenant:  k.tenant,
			Signals: []adminapi.ClusterSignalStorage{sig},
		})
	}

	return out
}
