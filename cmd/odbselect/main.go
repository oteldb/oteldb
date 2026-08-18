// Program odbselect serves oteldb's query APIs from a storage cluster it is not a member of.
//
// It is the read half of oteldb split into its own process, and the twin of odbingest: it follows
// cluster membership read-only, resolves each shard's owners through the ring, and reads from one
// of them. It holds no data and joins no ring, so query capacity scales independently of storage —
// adding a query node does not make it responsible for a share of the shards.
//
// PromQL, LogQL, TraceQL and the Pyroscope API are served by the same engines cmd/oteldb runs; only
// the seam beneath them differs. Ingestion, maintenance and the storage statistics views need
// engine-local state and are not served here.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Telemetry) error {
		ctx = zctx.WithOpenTelemetryZap(ctx)

		set := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
		cfgPath := set.String("config", "", "Path to config (defaults to odbselect.yml)")
		if err := set.Parse(os.Args[1:]); err != nil {
			return err
		}

		cfg, err := loadConfig(*cfgPath)
		if err != nil {
			return errors.Wrap(err, "load config")
		}
		if err := cfg.validate(); err != nil {
			return errors.Wrap(err, "invalid config")
		}

		root, err := newApp(ctx, cfg, lg, m)
		if err != nil {
			return errors.Wrap(err, "setup")
		}

		lg.Info("Starting odbselect")

		// The sdk's shutdown context covers SIGINT only, and a container runtime stops a pod with
		// SIGTERM — which the Go runtime's default disposition turns into an immediate exit, cutting
		// off in-flight queries. Canceling on it too is what lets a rolling restart drain.
		runCtx, stop := signal.NotifyContext(m.ShutdownContext(), syscall.SIGTERM)
		defer stop()

		return root.Run(runCtx)
	},
		app.WithServiceName("odbselect"),
	)
}
