// Command ch2storagebackend migrates data from chstorage's ClickHouse tables into the
// embedded storagebackend engine, by scanning ClickHouse directly and re-ingesting the
// decoded records as OTLP pdata. Logs, traces, and metrics are supported.
//
// A migration runs one UTC day at a time. Use -estimate first to size the window, -from/-to to
// select it, and -checkpoint to make the run resumable.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	sigstorage "github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"

	"github.com/oteldb/oteldb/internal/ch2storagebackend"
	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func run(ctx context.Context) error {
	var (
		dsn        = flag.String("dsn", "clickhouse://localhost:9000", "Clickhouse connection URL")
		storageDir = flag.String("storage-dir", "", "Directory for the embedded storage engine's file backend (empty uses an ephemeral in-memory backend)")
		batchSize  = flag.Int("batch", 5_000, "Number of records/spans to convert and ingest per batch")
		signals    = flag.String("signals", "logs,traces,metrics", "Comma-separated list of signals to migrate (logs, traces, metrics)")

		// Window selection. -from/-to are absolute and are what make a large migration schedulable:
		// the job can be split into ranges that are run separately, and re-run without redoing the
		// rest. -since remains as the "last N of data" shorthand.
		from  = flag.String("from", "", "Migrate data at or after this time (RFC3339 or YYYY-MM-DD, UTC); empty starts at the source's oldest")
		to    = flag.String("to", "", "Migrate data before this time (RFC3339 or YYYY-MM-DD, UTC); empty ends at the source's newest")
		since = flag.Duration("since", 0, "If positive and -from is unset, only migrate the last since of data, relative to the window's upper bound")

		// Pre-flight sizing: row counts per day and the source's compressed/uncompressed footprint,
		// without ingesting anything.
		estimate = flag.Bool("estimate", false, "Print a per-day sizing report for the selected window and exit without migrating")

		// Resumability. Days are journalled only after the target has been synced, so an interrupted
		// run resumes at a day boundary rather than restarting.
		checkpointPath = flag.String("checkpoint", "", "Path to a resumable migration journal; completed UTC days are skipped on a re-run")

		// A bulk migration writes orders of magnitude faster than steady production ingestion, so
		// the engine's default flush cadence (tuned for the latter) leaves the head/WAL growing
		// unbounded in RAM for the lifetime of this process. Flush aggressively instead.
		flushInterval = flag.Duration("flush-interval", 10*time.Second, "Max age of unflushed data in the embedded storage engine's head, for the file backend")
		// maxPartBytes caps a flushed/merged part's approximate uncompressed size, via the tenant
		// policy. It is what bounds the record engine's merge working set: a merge decodes at most
		// ~mergeHeight × maxPartBytes at a time, then seals. The default (0 ⇒ the engine's 64 MiB) is
		// tuned for steady production; a bulk backfill of large log rows should set this smaller (e.g.
		// 8-16 MiB) to keep merge-time RSS spikes low.
		maxPartBytes = flag.Int64("max-part-bytes", 0, "Approx uncompressed cap per flushed/merged part (0 = engine default 64MiB); lower it to bound merge-time memory on large backfills")
		// Throttling our own submission rate keeps unthrottled ingest from ballooning the head between
		// flushes (the head has no blocking backpressure; MaxInFlightBytes would *shed* records, which a
		// migration must not do). With a throttle the head stays small and the merge (bounded by
		// max-part-bytes) is the only sizable transient.
		throttle = flag.Duration("throttle", 0, "Sleep this long after every ConsumeLogs/ConsumeTraces batch, to keep ingestion from outpacing the storage engine's flush loop")
	)
	flag.Parse()

	window, err := parseWindow(*from, *to, *since)
	if err != nil {
		return err
	}

	lg, err := zap.NewDevelopment()
	if err != nil {
		return errors.Wrap(err, "create logger")
	}
	defer func() {
		_ = lg.Sync()
	}()
	ctx = zctx.Base(ctx, lg)

	client, err := chstorage.Dial(ctx, *dsn, chstorage.DialOptions{})
	if err != nil {
		return errors.Wrap(err, "dial clickhouse")
	}

	var checkpoint *ch2storagebackend.Checkpoint
	if *checkpointPath != "" {
		if checkpoint, err = ch2storagebackend.OpenCheckpoint(*checkpointPath); err != nil {
			return errors.Wrap(err, "open checkpoint")
		}
		defer func() {
			_ = checkpoint.Close()
		}()
	}

	// An estimate reads only ClickHouse metadata, so it must not open (and thereby lock or dirty)
	// the target storage directory.
	if *estimate {
		m := ch2storagebackend.NewMigrator(client, chstorage.DefaultTables(), nil, zap.NewNop(),
			ch2storagebackend.WithCheckpoint(checkpoint),
		)
		return printEstimates(ctx, m, *signals, window)
	}

	store, err := openStore(ctx, *storageDir, *flushInterval, *maxPartBytes, lg)
	if err != nil {
		return errors.Wrap(err, "open storage engine")
	}
	defer func() {
		_ = store.Close(ctx)
	}()
	back := storagebackend.New(store)

	m := ch2storagebackend.NewMigrator(client, chstorage.DefaultTables(), back, lg,
		ch2storagebackend.WithThrottle(*throttle),
		ch2storagebackend.WithCheckpoint(checkpoint),
		ch2storagebackend.WithSync(syncStore(store)),
	)

	for sig := range strings.SplitSeq(*signals, ",") {
		switch sig {
		case "logs":
			stats, err := m.MigrateLogs(ctx, window, *batchSize)
			if err != nil {
				return errors.Wrap(err, "migrate logs")
			}
			lg.Info("Migrated logs",
				zap.Int("records", stats.Records),
				zap.Int("batches", stats.Batches),
				zap.Int("days", stats.DaysDone),
				zap.Int("days_skipped", stats.DaysSkipped),
			)
		case "traces":
			stats, err := m.MigrateTraces(ctx, window, *batchSize)
			if err != nil {
				return errors.Wrap(err, "migrate traces")
			}
			lg.Info("Migrated traces",
				zap.Int("spans", stats.Spans),
				zap.Int("batches", stats.Batches),
				zap.Int("days", stats.DaysDone),
				zap.Int("days_skipped", stats.DaysSkipped),
			)
		case "metrics":
			stats, err := m.MigrateMetrics(ctx, window, *batchSize)
			if err != nil {
				return errors.Wrap(err, "migrate metrics")
			}
			lg.Info("Migrated metrics",
				zap.Int("points", stats.Points),
				zap.Int("exp_histograms", stats.ExpHistograms),
				zap.Int("batches", stats.Batches),
				zap.Int("days", stats.DaysDone),
				zap.Int("days_skipped", stats.DaysSkipped),
			)
		default:
			return errors.Errorf("unknown signal %q", sig)
		}
	}

	lg.Info("Done")
	return nil
}

// parseWindow builds the scan window from the time flags. Bare dates are accepted (and are the
// common case for a day-granular backfill) alongside full RFC3339 timestamps.
func parseWindow(from, to string, since time.Duration) (chstorage.Window, error) {
	parse := func(name, v string) (time.Time, error) {
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

	w := chstorage.Window{Since: since}

	var err error
	if w.From, err = parse("from", from); err != nil {
		return w, err
	}
	if w.To, err = parse("to", to); err != nil {
		return w, err
	}
	if !w.From.IsZero() && !w.To.IsZero() && !w.From.Before(w.To) {
		return w, errors.Errorf("-from %s must be before -to %s", w.From, w.To)
	}
	return w, nil
}

func printEstimates(ctx context.Context, m *ch2storagebackend.Migrator, signals string, w chstorage.Window) error {
	for sig := range strings.SplitSeq(signals, ",") {
		var (
			est ch2storagebackend.Estimate
			err error
		)
		switch sig {
		case "logs":
			est, err = m.EstimateLogs(ctx, w)
		case "traces":
			est, err = m.EstimateTraces(ctx, w)
		case "metrics":
			est, err = m.EstimateMetrics(ctx, w)
		default:
			return errors.Errorf("unknown signal %q", sig)
		}
		if err != nil {
			return errors.Wrapf(err, "estimate %s", sig)
		}
		_, _ = fmt.Fprint(os.Stdout, est)
	}
	return nil
}

// syncStore returns the per-day durability barrier: flush every tenant/signal head that has
// buffered data to an immutable part. Running it before a day is checkpointed is what makes the
// checkpoint safe to resume from — and it caps the head at one day's ingest rather than letting it
// grow for the whole migration.
func syncStore(store *storage.Storage) func(context.Context) error {
	return func(ctx context.Context) error {
		for _, t := range store.Inspect().Tenants {
			for _, sig := range t.Signals {
				if err := store.Admin().Flush(ctx, t.Tenant, sig.Signal); err != nil {
					return errors.Wrapf(err, "flush %s/%s", t.Tenant, sig.Signal)
				}
			}
		}
		return nil
	}
}

func openStore(ctx context.Context, dir string, flushInterval time.Duration, maxPartBytes int64, lg *zap.Logger) (*storage.Storage, error) {
	opts := []storage.Option{storage.WithLogger(lg)}
	if dir == "" {
		opts = append(opts,
			storage.WithBackend(backend.Memory()),
			storage.WithDurability(storage.DurabilityEphemeral),
		)
	} else {
		fb, err := backendfile.New(dir)
		if err != nil {
			return nil, errors.Wrap(err, "open file backend")
		}
		opts = append(opts,
			storage.WithBackend(fb),
			storage.WithFlushInterval(flushInterval.Nanoseconds()),
		)
	}

	// Bound the per-part size (hence the merge working set) via the tenant policy when requested. The
	// engine converts this to a row cap internally; a smaller cap seals sooner, keeping merge-time RSS
	// low on a bulk backfill of large rows.
	if maxPartBytes > 0 {
		opts = append(opts, storage.WithTenancy(tenant.ResolverFunc(func(sigstorage.TenantID) tenant.Policy {
			return tenant.Policy{Limits: tenant.Limits{MaxPartSize: maxPartBytes}}
		})))
	}

	return storage.Open(ctx, storage.Options{}, opts...)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
