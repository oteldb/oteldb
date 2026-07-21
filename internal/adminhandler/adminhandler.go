// Package adminhandler implements the oteldb admin panel API (see
// internal/adminapi) and serves a minimal embedded web UI on top of it.
package adminhandler

import (
	"context"
	"net/http"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// Component describes a wired oteldb service for the health report.
type Component struct {
	// Name is the service key (e.g. "loki", "otelcol").
	Name string
	// Addr is the listen address, when the component is an HTTP server.
	Addr string
	// Check optionally probes liveness; a nil Check reports the component as healthy.
	Check func(ctx context.Context) error
}

// Options configures the admin API handler.
type Options struct {
	// Info is static build information about the running binary.
	Info BuildInfo
	// StartTime is the process start time, used to compute uptime.
	StartTime time.Time
	// Engine provides embedded oteldb/storage statistics. It is the primary storage view and is nil
	// only when no signal is served from the embedded engine.
	Engine EngineStatsProvider
	// StorageBackend is the embedded engine backend ("memory", "file", "s3"), when Engine is set.
	StorageBackend string
	// Maintain triggers an on-demand embedded-storage maintenance cycle. Nil when unavailable.
	Maintain func(ctx context.Context) error
	// ClickHouseEnabled reports whether the deprecated ClickHouse storage is active.
	ClickHouseEnabled bool
	// Signals describes per-signal backend configuration.
	Signals []adminapi.SignalInfo
	// Components lists the wired services for the health report.
	Components []Component
	// CHStorage collects deprecated ClickHouse storage statistics. Nil when no signal is served from
	// ClickHouse.
	CHStorage StorageStatsCollector
}

// EngineStatsProvider exposes an in-memory snapshot of embedded-storage engine statistics.
type EngineStatsProvider interface {
	Inspect() storage.StoreStats
}

// BuildInfo is static build information about the running binary.
type BuildInfo struct {
	Version   string
	Commit    string
	GoVersion string
}

// StorageStatsCollector collects per-table ClickHouse storage statistics.
type StorageStatsCollector interface {
	CollectStorageStats(ctx context.Context) ([]adminapi.TableStats, error)
}

// AdminAPI implements adminapi.Handler.
type AdminAPI struct {
	opts Options
}

var _ adminapi.Handler = (*AdminAPI)(nil)

// NewAdminAPI creates a new admin API handler.
func NewAdminAPI(opts Options) *AdminAPI {
	return &AdminAPI{opts: opts}
}

// GetInfo implements getInfo operation.
func (a *AdminAPI) GetInfo(_ context.Context) (*adminapi.InstanceInfo, error) {
	info := &adminapi.InstanceInfo{
		Version:           a.opts.Info.Version,
		Commit:            a.opts.Info.Commit,
		GoVersion:         a.opts.Info.GoVersion,
		Os:                runtime.GOOS,
		Arch:              runtime.GOARCH,
		StartTime:         a.opts.StartTime,
		UptimeSeconds:     time.Since(a.opts.StartTime).Seconds(),
		StorageEnabled:    a.opts.Engine != nil,
		ClickhouseEnabled: a.opts.ClickHouseEnabled,
		Signals:           a.opts.Signals,
	}
	if a.opts.StorageBackend != "" {
		info.StorageBackend = adminapi.NewOptString(a.opts.StorageBackend)
	}
	return info, nil
}

// GetHealth implements getHealth operation.
func (a *AdminAPI) GetHealth(ctx context.Context) (*adminapi.HealthReport, error) {
	components := make([]adminapi.ComponentHealth, 0, len(a.opts.Components))
	overall := adminapi.HealthStatusHealthy
	for _, c := range a.opts.Components {
		ch := adminapi.ComponentHealth{
			Name:   c.Name,
			Status: adminapi.HealthStatusHealthy,
		}
		if c.Addr != "" {
			ch.Addr = adminapi.NewOptString(c.Addr)
		}
		if c.Check != nil {
			if err := c.Check(ctx); err != nil {
				ch.Status = adminapi.HealthStatusUnhealthy
				ch.Error = adminapi.NewOptString(err.Error())
				overall = adminapi.HealthStatusDegraded
			}
		}
		components = append(components, ch)
	}
	return &adminapi.HealthReport{
		Status:     overall,
		Components: components,
	}, nil
}

// GetRuntime implements getRuntime operation.
func (a *AdminAPI) GetRuntime(_ context.Context) (*adminapi.RuntimeStats, error) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	stats := &adminapi.RuntimeStats{
		Goroutines:      int64(runtime.NumGoroutine()),
		NumCPU:          runtime.NumCPU(),
		Gomaxprocs:      runtime.GOMAXPROCS(0),
		HeapAllocBytes:  int64(ms.HeapAlloc),
		HeapInuseBytes:  int64(ms.HeapInuse),
		HeapSysBytes:    int64(ms.HeapSys),
		StackInuseBytes: int64(ms.StackInuse),
		GcCount:         int64(ms.NumGC),
		NextGcBytes:     int64(ms.NextGC),
	}
	if limit := debug.SetMemoryLimit(-1); limit != memLimitOff {
		stats.MemLimitBytes = adminapi.NewOptInt64(limit)
	}
	return stats, nil
}

// memLimitOff is the value returned by debug.SetMemoryLimit when GOMEMLIMIT is unset.
const memLimitOff = int64(1<<63 - 1)

// GetStorage implements getStorage operation.
func (a *AdminAPI) GetStorage(ctx context.Context) (*adminapi.StorageStats, error) {
	stats := &adminapi.StorageStats{
		StorageEnabled:    a.opts.Engine != nil,
		ClickhouseEnabled: a.opts.ClickHouseEnabled,
	}
	if a.opts.Engine != nil {
		stats.Engine = adminapi.NewOptEngineStats(mapEngineStats(a.opts.Engine.Inspect()))
	}
	if a.opts.CHStorage != nil {
		tables, err := a.opts.CHStorage.CollectStorageStats(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "collect clickhouse storage stats")
		}
		stats.Clickhouse = adminapi.NewOptClickHouseStats(adminapi.ClickHouseStats{Tables: tables})
	}
	return stats, nil
}

// RunAction implements runAction operation.
func (a *AdminAPI) RunAction(ctx context.Context, params adminapi.RunActionParams) (*adminapi.ActionResult, error) {
	switch params.Action {
	case adminapi.ActionNameGc:
		before := heapAlloc()
		runtime.GC()
		freed := saturatingSub(before, heapAlloc())
		return &adminapi.ActionResult{
			Action:     params.Action,
			Ok:         true,
			Message:    "GC completed",
			FreedBytes: adminapi.NewOptInt64(freed),
		}, nil
	case adminapi.ActionNameFreeOsMemory:
		before := heapAlloc()
		debug.FreeOSMemory()
		freed := saturatingSub(before, heapAlloc())
		return &adminapi.ActionResult{
			Action:     params.Action,
			Ok:         true,
			Message:    "released free memory to the OS",
			FreedBytes: adminapi.NewOptInt64(freed),
		}, nil
	case adminapi.ActionNameStorageMaintain:
		if a.opts.Maintain == nil {
			return nil, errors.New("embedded storage is not enabled")
		}
		if err := a.opts.Maintain(ctx); err != nil {
			return nil, errors.Wrap(err, "run storage maintenance")
		}
		return &adminapi.ActionResult{
			Action:  params.Action,
			Ok:      true,
			Message: "storage maintenance cycle completed",
		}, nil
	default:
		return nil, errors.Errorf("unknown action %q", params.Action)
	}
}

func heapAlloc() int64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc)
}

func saturatingSub(a, b int64) int64 {
	if a < b {
		return 0
	}
	return a - b
}

// NewError creates *ErrorStatusCode from error returned by handler.
func (a *AdminAPI) NewError(_ context.Context, err error) *adminapi.ErrorStatusCode {
	return &adminapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response: adminapi.Error{
			ErrorMessage: err.Error(),
		},
	}
}
