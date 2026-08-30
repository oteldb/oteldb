// Program odbadmin serves the admin API and web UI for a whole storage cluster.
//
// It is the cluster-wide view the per-node admin panel cannot give: each storage node reports only
// its own engine, so on a three-node cluster the storage page shows a third of the picture and the
// ingest and flush counters land on whichever node happened to do the work. odbadmin joins etcd for
// membership the way odbselect does, fans out to every member's admin API, and folds the answers
// into one report — including, at /api/v1/cluster/storage, the footprint counted both per stored
// copy and per distinct part, because replication makes those two different numbers.
//
// It holds no data, joins no ring, and is read-only: maintenance actions still belong to the node
// that would run them.
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
		cfgPath := set.String("config", "", "Path to config (defaults to odbadmin.yml)")
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

		lg.Info("Starting odbadmin")

		// The sdk's shutdown context covers SIGINT only, and a container runtime stops a pod with
		// SIGTERM — which the Go runtime's default disposition turns into an immediate exit. Canceling
		// on it too is what lets a rolling restart drain.
		runCtx, stop := signal.NotifyContext(m.ShutdownContext(), syscall.SIGTERM)
		defer stop()

		return root.Run(runCtx)
	},
		app.WithServiceName("odbadmin"),
	)
}
