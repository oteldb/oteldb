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
	}
	for _, t := range s.Tenants {
		out.Tenants = append(out.Tenants, mapTenantStats(t))
	}
	if c := s.Cluster; c != nil {
		out.Cluster = adminapi.NewOptClusterStats(mapClusterStats(*c))
	}
	return out
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
	return cs
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
