package clusteradmin

import (
	"cmp"
	"context"
	"slices"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// GetEfficiency implements getEfficiency operation.
//
// The figures are summed across the nodes that answered, so a part stored on two owners is counted
// twice — this schema describes one node's footprint and has no place to say otherwise.
// /api/v1/cluster/storage reports the deduplicated total alongside this one.
//
// The per-part listing is not forwarded: an individual part belongs to the node holding it, and the
// same part would appear once per replica here.
func (a *Aggregator) GetEfficiency(ctx context.Context, _ adminapi.GetEfficiencyParams) (*adminapi.EfficiencyStats, error) {
	answers, err := fanout(ctx, a, "storage/efficiency",
		func(ctx context.Context, p Peer) (*adminapi.EfficiencyStats, error) {
			return p.Client.GetEfficiency(ctx, adminapi.GetEfficiencyParams{})
		},
	)
	if err != nil {
		return nil, err
	}

	stats := &adminapi.EfficiencyStats{Tenants: []adminapi.TenantEfficiency{}}

	tenants := map[string]*adminapi.TenantEfficiency{}
	var order []string

	for _, r := range answers {
		if !r.ok() {
			continue
		}

		stats.StorageEnabled = stats.StorageEnabled || r.Value.StorageEnabled

		for _, t := range r.Value.Tenants {
			into, seen := tenants[t.Tenant]
			if !seen {
				into = &adminapi.TenantEfficiency{Tenant: t.Tenant, Signals: []adminapi.SignalEfficiency{}}
				tenants[t.Tenant] = into
				order = append(order, t.Tenant)
			}
			addTenantEfficiency(into, t)
		}
	}

	slices.Sort(order)
	for _, name := range order {
		te := tenants[name]
		for i := range te.Signals {
			finishSignalEfficiency(&te.Signals[i])
		}
		stats.Tenants = append(stats.Tenants, *te)
	}

	return stats, nil
}

func addTenantEfficiency(into *adminapi.TenantEfficiency, from adminapi.TenantEfficiency) {
	for _, s := range from.Signals {
		s.PartsDetail = nil

		i, found := slices.BinarySearchFunc(into.Signals, s.Signal,
			func(e adminapi.SignalEfficiency, want adminapi.Signal) int { return cmp.Compare(e.Signal, want) },
		)
		if found {
			addSignalEfficiency(&into.Signals[i], s)

			continue
		}
		into.Signals = slices.Insert(into.Signals, i, s)
	}
}

func addSignalEfficiency(into *adminapi.SignalEfficiency, from adminapi.SignalEfficiency) {
	into.Series += from.Series
	into.Parts += from.Parts
	into.Points += from.Points
	into.StoredBytes += from.StoredBytes

	if lb, ok := from.LogicalBytes.Get(); ok {
		cur, _ := into.LogicalBytes.Get()
		into.LogicalBytes = adminapi.NewOptInt64(cur + lb)
	}
}

// finishSignalEfficiency recomputes the derived ratios from the summed totals. Averaging the nodes'
// own ratios would weight a node holding one part the same as one holding a thousand.
func finishSignalEfficiency(se *adminapi.SignalEfficiency) {
	se.BytesPerPoint = 0
	if se.Points > 0 {
		se.BytesPerPoint = float64(se.StoredBytes) / float64(se.Points)
	}

	se.CompressionRatio.Reset()
	if lb, ok := se.LogicalBytes.Get(); ok && lb > 0 && se.StoredBytes > 0 {
		se.CompressionRatio = adminapi.NewOptFloat64(float64(lb) / float64(se.StoredBytes))
	}
}
