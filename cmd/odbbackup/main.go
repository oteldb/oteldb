// Command odbbackup writes an oteldb backup to a directory.
//
// It speaks two backends, selected with -backend: "clickhouse" (the default) dumps chstorage's
// tables in ClickHouse Native format, and "storage" dumps the embedded storage engine through its
// read seam. The two are separate formats — a backup restores into the backend it came from — but
// they are one command so an operator has one tool to learn.
//
// The storage backend is opened read-only: no WAL recovery, no flush, no merges, no retention and
// no cluster membership, so backing up a data directory does not modify it. The cost is that data
// still in the unflushed head (the WAL) is not backed up; keep -lag at or above the engine's flush
// interval so the window ends behind whatever the head holds.
//
// See internal/storagebackup for the storage backend's layout and its fidelity contract.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/storagebackup"
)

func run(ctx context.Context) error {
	var (
		backend = flag.String("backend", "clickhouse", "Storage backend to back up: clickhouse or storage")
		path    = flag.String("path", "./backup", "Backup destination directory")

		dsn = flag.String("dsn", "clickhouse://localhost:9000", "Clickhouse connection URL (clickhouse backend)")

		storageConfig = flag.String("storage-config", "", "oteldb config file whose storage block describes the engine (storage backend)")
		storageDir    = flag.String("storage-dir", "", "Data directory of a single-node file backend, instead of -storage-config; opened read-only (storage backend)")
		signals       = flag.String("signals", "", "Comma-separated signals to back up: log, trace, metric (default: all, storage backend)")
		from          = flag.String("from", "", "Back up data at or after this time (RFC3339 or YYYY-MM-DD, UTC); empty starts at the oldest retained")
		to            = flag.String("to", "", "Back up data before this time (RFC3339 or YYYY-MM-DD, UTC); empty ends at now minus -lag")
		lag           = flag.Duration("lag", storagebackup.DefaultLag, "Keep the window this far behind the ingest edge, so the newest day is not scanned while it fills")
		resume        = flag.Bool("resume", false, "Skip days already present in the destination, resuming an interrupted backup")
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
		backup := chstorage.NewBackup(d, chstorage.DefaultTables(), lg.Named("backup"))

		return backup.Create(ctx, *path)
	case "storage":
		opts := storagebackup.BackupOptions{Lag: *lag, Resume: *resume}
		if opts.Signals, err = storagebackup.ParseSignals(*signals); err != nil {
			return err
		}
		if opts.From, err = parseTime("from", *from); err != nil {
			return err
		}
		if opts.To, err = parseTime("to", *to); err != nil {
			return err
		}
		if !opts.From.IsZero() && !opts.To.IsZero() && !opts.From.Before(opts.To) {
			return errors.Errorf("-from %s must be before -to %s", opts.From, opts.To)
		}

		back, stop, err := storagebackup.OpenEngine(ctx, storagebackup.EngineConfig{
			Path:     *storageConfig,
			Dir:      *storageDir,
			ReadOnly: true,
		}, lg)
		if err != nil {
			return err
		}
		defer func() {
			_ = stop(ctx)
		}()

		store := back.Store()
		if store == nil {
			return errors.New("configured storage backend has no local engine to back up")
		}

		stats, err := storagebackup.NewBackup(store, lg.Named("backup"), opts).Create(ctx, *path)
		if err != nil {
			return errors.Wrap(err, "back up storage")
		}
		lg.Info("Backed up storage",
			zap.Int("files", stats.Files),
			zap.Int("skipped", stats.Skipped),
			zap.Int("streams", stats.Streams),
			zap.Int("chunks", stats.Chunks),
			zap.Int("rows", stats.Rows),
		)
		return nil
	default:
		return errors.Errorf("unknown backend %q", *backend)
	}
}

// parseTime accepts a bare UTC date alongside a full timestamp, since a day-granular window is the
// common case.
func parseTime(name, v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{time.RFC3339, time.DateTime, time.DateOnly} {
		if t, err := time.ParseInLocation(layout, v, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, errors.Errorf("parse -%s %q: want RFC3339 or YYYY-MM-DD", name, v)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
