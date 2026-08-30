package clusteradmin

import (
	"cmp"
	"context"
	"runtime"
	"slices"
	"time"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// GetInfo implements getInfo operation.
//
// The build and uptime fields describe the aggregator process — it is the one answering, and a
// cluster has no single version to report. Everything below them is the cluster's: a capability is
// enabled when any member has it, and the signal list is the union of what the members serve.
func (a *Aggregator) GetInfo(ctx context.Context) (*adminapi.InstanceInfo, error) {
	answers, err := fanout(ctx, a, "info",
		func(ctx context.Context, p Peer) (*adminapi.InstanceInfo, error) { return p.Client.GetInfo(ctx) },
	)
	if err != nil {
		return nil, err
	}

	info := &adminapi.InstanceInfo{
		Version:       a.opts.Info.Version,
		Commit:        a.opts.Info.Commit,
		GoVersion:     a.opts.Info.GoVersion,
		Os:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		StartTime:     a.opts.StartTime,
		UptimeSeconds: time.Since(a.opts.StartTime).Seconds(),
		Signals:       []adminapi.SignalInfo{},
	}

	signals := map[adminapi.Signal]adminapi.SignalInfo{}
	backends := map[string]struct{}{}

	for _, r := range answers {
		if !r.ok() {
			continue
		}

		info.StorageEnabled = info.StorageEnabled || r.Value.StorageEnabled
		info.ClickhouseEnabled = info.ClickhouseEnabled || r.Value.ClickhouseEnabled

		if b, ok := r.Value.StorageBackend.Get(); ok {
			backends[b] = struct{}{}
		}

		for _, s := range r.Value.Signals {
			mergeSignalInfo(signals, s)
		}
	}

	// A single backend name describes the cluster; a mixed one describes none of it, so it is left
	// out rather than reported as whichever node happened to be asked first.
	if len(backends) == 1 {
		for b := range backends {
			info.StorageBackend = adminapi.NewOptString(b)
		}
	}

	for _, s := range signals {
		info.Signals = append(info.Signals, s)
	}
	slices.SortFunc(info.Signals, func(a, b adminapi.SignalInfo) int { return cmp.Compare(a.Signal, b.Signal) })

	return info, nil
}

// mergeSignalInfo folds one node's view of a signal into the cluster's. A signal is queryable when
// any node can answer for it, and "none" is only the backend when no node named a real one.
func mergeSignalInfo(into map[adminapi.Signal]adminapi.SignalInfo, s adminapi.SignalInfo) {
	cur, seen := into[s.Signal]
	if !seen {
		into[s.Signal] = s

		return
	}

	if cur.Backend == "none" {
		cur.Backend = s.Backend
	}
	if s.Queryable && !cur.Queryable {
		cur.Queryable, cur.Bind = true, s.Bind
	}

	into[s.Signal] = cur
}
