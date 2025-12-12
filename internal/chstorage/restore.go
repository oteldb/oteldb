package chstorage

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/go-faster/errors"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Restore implements a oteldb restore process.
//
// Restore is stored in a Clickhouse native format.
type Restore struct {
	client ClickHouseClient
	tables Tables
	logger *zap.Logger
}

// NewRestore creates a new [Restore] instance.
func NewRestore(client ClickHouseClient, tables Tables, logger *zap.Logger) *Restore {
	return &Restore{
		client: client,
		tables: tables,
		logger: logger,
	}
}

// Restore performs restore from the given directory.
func (b *Restore) Restore(ctx context.Context, dir string) error {
	if err := b.tables.CreateNonDestructive(ctx, b.client); err != nil {
		return errors.Wrap(err, "create tables")
	}
	var (
		metrics = metricsRestore{
			client:         b.client,
			tables:         b.tables,
			logger:         b.logger.Named("metrics"),
			timeseries:     newTimeseriesColumns(),
			seenTimeseries: map[[16]byte]struct{}{},
			labels:         map[[2]string]labelScope{},
		}
		traces = tracesRestore{
			client: b.client,
			tables: b.tables,
			logger: b.logger.Named("traces"),
		}
		logs = logsRestore{
			client: b.client,
			tables: b.tables,
			logger: b.logger.Named("logs"),
		}
	)
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx
		if err := metrics.Do(ctx, filepath.Join(dir, "metrics")); err != nil {
			return errors.Wrap(err, "restore metrics")
		}
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		if err := traces.Do(ctx, filepath.Join(dir, "traces")); err != nil {
			return errors.Wrap(err, "restore traces")
		}
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		if err := logs.Do(ctx, filepath.Join(dir, "logs")); err != nil {
			return errors.Wrap(err, "restore logs")
		}
		return nil
	})
	return grp.Wait()
}

func openBackupReader(dir, name string) (io.ReadCloser, error) {
	dumpPath := filepath.Join(dir, name+".native.zstd")

	f, err := os.Open(filepath.Clean(dumpPath))
	if err != nil {
		return nil, errors.Wrap(err, "open dump file")
	}

	dec, err := zstd.NewReader(f)
	if err != nil {
		return nil, errors.Wrap(err, "make zstd decoder")
	}

	rc := struct {
		io.Reader
		io.Closer
	}{
		Reader: dec,
		Closer: f,
	}
	return rc, nil
}
