package adminhandler

import (
	"time"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// mapEngineStats converts an embedded-storage snapshot into the admin API representation.
func mapEngineStats(s storage.StoreStats) adminapi.EngineStats {
	out := adminapi.EngineStats{
		Tenants: make([]adminapi.TenantStats, 0, len(s.Tenants)),
		Caches: adminapi.CacheStats{
			DecodeCache: adminapi.DecodeCacheStats{
				Hits:   s.Caches.Decode.Hits,
				Misses: s.Caches.Decode.Misses,
				Bytes:  s.Caches.Decode.Bytes,
				Items:  int64(s.Caches.Decode.Items),
			},
		},
		Maintenance: mapMaintenanceStats(s.Maintenance),
	}
	for _, t := range s.Tenants {
		out.Tenants = append(out.Tenants, mapTenantStats(t))
	}
	if c := s.Cluster; c != nil {
		out.Cluster = adminapi.NewOptClusterStats(mapClusterStats(*c))
	}
	return out
}

func mapMaintenanceStats(m storage.MaintenanceStats) adminapi.MaintenanceStats {
	ms := adminapi.MaintenanceStats{
		Cycles:                   m.Cycles,
		LastCycleDurationSeconds: time.Duration(m.LastCycleDurationNano).Seconds(),
		LastCycleTasks:           m.LastCycleTasks,
	}
	if m.LastCycleStartUnixNano > 0 {
		ms.LastCycleStart = adminapi.NewOptDateTime(time.Unix(0, m.LastCycleStartUnixNano).UTC())
	}
	return ms
}

func mapTenantStats(t storage.TenantStats) adminapi.TenantStats {
	ts := adminapi.TenantStats{
		Tenant: string(t.Tenant),
		Admission: adminapi.AdmissionStats{
			Accepted:            t.Admission.Accepted,
			RejectedOoo:         t.Admission.RejectedOOO,
			RejectedRate:        t.Admission.RejectedRate,
			RejectedCardinality: t.Admission.RejectedCardinality,
			RejectedInFlight:    t.Admission.RejectedInFlight,
			SampledDropped:      t.Admission.SampledDropped,
			Overflowed:          t.Admission.Overflowed,
		},
		Signals: make([]adminapi.EngineSignalStats, 0, len(t.Signals)),
	}
	for _, s := range t.Signals {
		ts.TotalSeries += s.Series
		ts.TotalParts += int64(s.Parts)
		ts.Signals = append(ts.Signals, mapSignalStats(s))
	}
	return ts
}

func mapSignalStats(s storage.SignalStats) adminapi.EngineSignalStats {
	es := adminapi.EngineSignalStats{
		Signal:       mapSignal(s.Signal),
		Series:       s.Series,
		HeadItems:    s.HeadItems,
		HeadBytes:    s.HeadBytes,
		Parts:        int64(s.Parts),
		MergeRunning: s.MergeRunning,
		MergeBacklog: int64(s.MergeBacklog),
		Wal:          s.WAL,
		WalSegments:  int64(s.WALSegments),
		WalBytes:     s.WALBytes,
	}
	if s.MinTimeUnixNano > 0 {
		es.MinTime = adminapi.NewOptDateTime(time.Unix(0, s.MinTimeUnixNano).UTC())
	}
	if s.MaxTimeUnixNano > 0 {
		es.MaxTime = adminapi.NewOptDateTime(time.Unix(0, s.MaxTimeUnixNano).UTC())
	}
	return es
}

func mapClusterStats(c storage.ClusterStats) adminapi.ClusterStats {
	cs := adminapi.ClusterStats{
		Self:    c.Self,
		Owned:   c.Owned,
		Members: make([]adminapi.ClusterMember, 0, len(c.Members)),
	}
	if cs.Owned == nil {
		cs.Owned = []string{}
	}
	for _, m := range c.Members {
		cm := adminapi.ClusterMember{ID: m.ID}
		if m.Zone != "" {
			cm.Zone = adminapi.NewOptString(m.Zone)
		}
		if m.Addr != "" {
			cm.Addr = adminapi.NewOptString(m.Addr)
		}
		cs.Members = append(cs.Members, cm)
	}
	if ps := c.PartSync; ps != nil {
		cs.PartSync = adminapi.NewOptPartSyncStats(mapPartSyncStats(*ps))
	}
	if ec := c.EC; ec != nil {
		cs.Ec = adminapi.NewOptECStats(mapECStats(*ec))
	}
	return cs
}

func mapPartSyncStats(ps storage.PartSyncStats) adminapi.PartSyncStats {
	out := adminapi.PartSyncStats{
		Passes:      ps.Passes,
		Mirrored:    ps.Mirrored,
		Copied:      ps.Copied,
		CopiedBytes: ps.CopiedBytes,
		Pruned:      ps.Pruned,
		Errors:      ps.Errors,
	}
	if ps.LastSyncUnixNano > 0 {
		out.LastSync = adminapi.NewOptDateTime(time.Unix(0, ps.LastSyncUnixNano).UTC())
	}
	return out
}

func mapECStats(ec storage.ECStats) adminapi.ECStats {
	return adminapi.ECStats{
		Converted:         ec.Converted,
		ConvertErrors:     ec.ConvertErrors,
		RepairedSlots:     ec.RepairedSlots,
		RepairErrors:      ec.RepairErrors,
		PrunedStagedParts: ec.PrunedStagedParts,
		Reconstructs:      ec.Reconstructs,
		ReconstructErrors: ec.ReconstructErrors,
	}
}

func mapTenantEfficiency(t storage.TenantEfficiency) adminapi.TenantEfficiency {
	te := adminapi.TenantEfficiency{
		Tenant:  string(t.Tenant),
		Signals: make([]adminapi.SignalEfficiency, 0, len(t.Signals)),
	}
	for _, s := range t.Signals {
		se := adminapi.SignalEfficiency{
			Signal:        mapSignal(s.Signal),
			Series:        s.Series,
			Parts:         int64(s.Parts),
			Points:        s.Points,
			StoredBytes:   s.StoredBytes,
			BytesPerPoint: s.BytesPerPoint,
		}
		if s.LogicalBytes > 0 {
			se.LogicalBytes = adminapi.NewOptInt64(s.LogicalBytes)
		}
		if s.CompressionRatio > 0 {
			se.CompressionRatio = adminapi.NewOptFloat64(s.CompressionRatio)
		}
		te.Signals = append(te.Signals, se)
	}
	return te
}

// mapSignal maps a storage signal (singular names) onto the admin API signal enum (plural names).
func mapSignal(s signal.Signal) adminapi.Signal {
	switch s {
	case signal.Metric:
		return adminapi.SignalMetrics
	case signal.Log:
		return adminapi.SignalLogs
	case signal.Trace:
		return adminapi.SignalTraces
	case signal.Profile:
		return adminapi.SignalProfiles
	default:
		return adminapi.Signal(s.String())
	}
}
