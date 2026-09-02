package storagebackup

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// EngineConfig selects the storage engine odbbackup reads from, or odbrestore writes to.
type EngineConfig struct {
	// Path is an oteldb config file; its "storage" block configures the engine. It is the way to
	// reach a clustered engine, which is the case that matters: joining the destination cluster is
	// what lets a restore's writes be routed and sharded by the ring rather than landing locally.
	Path string
	// Dir is the shorthand for a single-node file backend rooted there, for an offline engine that
	// needs no other configuration. Ignored when Path is set.
	Dir string
}

// engineFile is the "storage" block of an oteldb config file.
type engineFile struct {
	Storage storagebackend.Config `json:"storage" yaml:"storage"`
}

// OpenEngine opens the engine described by cfg. The returned func stops and flushes it.
func OpenEngine(
	ctx context.Context, cfg EngineConfig, lg *zap.Logger,
) (*storagebackend.Backend, func(context.Context) error, error) {
	var scfg storagebackend.Config
	switch {
	case cfg.Path != "":
		file, err := config.Load[engineFile](cfg.Path, config.LoadOptions{})
		if err != nil {
			return nil, nil, errors.Wrap(err, "load config")
		}
		scfg = file.Storage
	case cfg.Dir != "":
		scfg = storagebackend.Config{Backend: "file", Dir: cfg.Dir}
	default:
		return nil, nil, errors.New("one of -storage-config or -storage-dir is required")
	}
	scfg.SetDefaults()

	// The telemetry is only a provider holder here; its zero value falls back to the global
	// providers, which a one-shot command has no reason to configure.
	back, stop, err := storagebackend.Open(ctx, scfg, lg, &app.Telemetry{})
	if err != nil {
		return nil, nil, errors.Wrap(err, "open storage engine")
	}
	return back, stop, nil
}
