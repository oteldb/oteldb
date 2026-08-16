// Program odbingest ingests telemetry into an oteldb storage cluster.
//
// It is the write half of oteldb split into its own process: it accepts Prometheus remote write
// (with the OTLP signals to follow), converts it straight into the engine's ingest model, and
// routes each shard to its ring primary — no collector pipeline and no pdata in between.
//
// It is stateless. It follows cluster membership read-only, never joins the ring, and holds no
// data: a request is answered only once the shards it touched have taken it, so scaling odbingest
// is independent of scaling storage.
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
		cfgPath := set.String("config", "", "Path to config (defaults to odbingest.yml)")
		if err := set.Parse(os.Args[1:]); err != nil {
			return err
		}

		cfg, err := loadConfig(*cfgPath)
		if err != nil {
			return errors.Wrap(err, "load config")
		}

		root, err := newApp(ctx, cfg, lg, m)
		if err != nil {
			return errors.Wrap(err, "setup")
		}

		lg.Info("Starting odbingest")

		// The sdk's shutdown context covers SIGINT only, and a container runtime stops a pod with
		// SIGTERM — which the Go runtime's default disposition turns into an immediate exit,
		// cutting off in-flight writes the cluster may already have taken. Canceling on it too is
		// what lets a rolling restart drain instead of leaving senders to retry.
		runCtx, stop := signal.NotifyContext(m.ShutdownContext(), syscall.SIGTERM)
		defer stop()

		return root.Run(runCtx)
	},
		app.WithServiceName("odbingest"),
	)
}
