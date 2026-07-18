// Command ch2storagebackend migrates data from chstorage's ClickHouse tables into the
// embedded storagebackend engine, by scanning ClickHouse directly and re-ingesting the
// decoded records as OTLP pdata. Only logs and traces are supported so far.
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
		since      = flag.Duration("since", 0, "If positive, only migrate the last since of data (relative to the most recent record), instead of the full table")
		signals    = flag.String("signals", "logs,traces", "Comma-separated list of signals to migrate (logs, traces)")
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

	store, err := openStore(ctx, *storageDir, *flushInterval, *maxPartBytes, lg)
	if err != nil {
		return errors.Wrap(err, "open storage engine")
	}
	defer func() {
		_ = store.Close(ctx)
	}()
	back := storagebackend.New(store)

	m := ch2storagebackend.NewMigrator(client, chstorage.DefaultTables(), back, lg, ch2storagebackend.WithThrottle(*throttle))

	for sig := range strings.SplitSeq(*signals, ",") {
		switch sig {
		case "logs":
			stats, err := m.MigrateLogs(ctx, *since, *batchSize)
			if err != nil {
				return errors.Wrap(err, "migrate logs")
			}
			lg.Info("Migrated logs", zap.Int("records", stats.Records), zap.Int("batches", stats.Batches))
		case "traces":
			stats, err := m.MigrateTraces(ctx, *since, *batchSize)
			if err != nil {
				return errors.Wrap(err, "migrate traces")
			}
			lg.Info("Migrated traces", zap.Int("spans", stats.Spans), zap.Int("batches", stats.Batches))
		default:
			return errors.Errorf("unknown signal %q", sig)
		}
	}

	lg.Info("Done")
	return nil
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
