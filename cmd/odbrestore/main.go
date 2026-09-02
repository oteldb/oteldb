// Command odbrestore restores an oteldb backup written by odbbackup.
//
// It speaks two backends, selected with -backend: "clickhouse" (the default) inserts chstorage's
// Native dumps back into ClickHouse, and "storage" re-ingests a storage-engine backup through the
// engine's ordinary write path. Going through the write path is the point of the storage restore:
// tenant routing and, in cluster mode, the shard key and its ring placement are derived again from
// the *destination's* configuration, so the same tool covers disaster recovery, a
// shards_per_tenant change, and moving a tenant between clusters.
//
// A restore writes: it needs the destination engine to itself, and must not be pointed at the data
// directory of a running node, which would put two writers on one backend. Restore into a stopped
// node (or a fresh directory) and start it afterwards.
//
// See internal/storagebackup for the layout and the fidelity contract.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	sigstorage "github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/storagebackup"
)

func run(ctx context.Context) error {
	var (
		backend = flag.String("backend", "clickhouse", "Storage backend to restore into: clickhouse or storage")
		path    = flag.String("path", "./restore", "Backup directory")

		dsn = flag.String("dsn", "clickhouse://localhost:9000", "Clickhouse connection URL (clickhouse backend)")

		storageConfig = flag.String("storage-config", "", "oteldb config file whose storage block describes the destination engine (storage backend)")
		storageDir    = flag.String("storage-dir", "", "Data directory of a single-node file backend, instead of -storage-config; must not be a running node's (storage backend)")
		signals       = flag.String("signals", "", "Comma-separated signals to restore: log, trace, metric (default: all, storage backend)")
		tenant        = flag.String("tenant", "", "Restore only this logical tenant from the backup (default: all)")
		batch         = flag.Int("batch", storagebackup.DefaultRestoreBatchSize, "Records, spans or samples buffered per write")
	)
	flag.Parse()

	lg, err := zap.NewDevelopment()
	if err != nil {
		return errors.Wrap(err, "create logger")
	}
	defer func() {
		_ = lg.Sync()
	}()
	ctx = zctx.Base(ctx, lg)

	switch *backend {
	case "clickhouse":
		d, err := chstorage.Dial(ctx, *dsn, chstorage.DialOptions{})
		if err != nil {
			return errors.Wrap(err, "dial clickhouse")
		}
		restore := chstorage.NewRestore(d, chstorage.DefaultTables(), lg.Named("restore"))

		return restore.Restore(ctx, *path)
	case "storage":
		opts := storagebackup.RestoreOptions{
			Tenant:    sigstorage.TenantID(*tenant),
			BatchSize: *batch,
		}
		if opts.Signals, err = storagebackup.ParseSignals(*signals); err != nil {
			return err
		}

		back, stop, err := storagebackup.OpenEngine(ctx, storagebackup.EngineConfig{
			Path: *storageConfig,
			Dir:  *storageDir,
		}, lg)
		if err != nil {
			return err
		}
		defer func() {
			_ = stop(ctx)
		}()

		stats, err := storagebackup.NewRestore(back, lg.Named("restore"), opts).Restore(ctx, *path)
		if err != nil {
			return errors.Wrap(err, "restore storage")
		}
		lg.Info("Restored storage",
			zap.Int("files", stats.Files),
			zap.Int("streams", stats.Streams),
			zap.Int("rows", stats.Rows),
			zap.Int("batches", stats.Batches),
		)
		return nil
	default:
		return errors.Errorf("unknown backend %q", *backend)
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
