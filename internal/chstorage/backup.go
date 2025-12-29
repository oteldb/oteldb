package chstorage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
)

// Backup implements a oteldb backup process.
//
// Backup is stored in a Clickhouse Native format.
type Backup struct {
	client ClickHouseClient
	tables Tables
	logger *zap.Logger
}

// NewBackup creates a new [Backup] instance.
func NewBackup(client ClickHouseClient, tables Tables, logger *zap.Logger) *Backup {
	return &Backup{
		client: client,
		tables: tables,
		logger: logger,
	}
}

// Create creates a backup in the specified directory.
func (b *Backup) Create(ctx context.Context, dir string) error {
	db, err := queryCurrentDatabase(ctx, b.client)
	if err != nil {
		return errors.Wrap(err, "query current database")
	}

	var (
		metrics = metricsBackup{
			client:   b.client,
			tables:   b.tables,
			database: db,
			logger:   b.logger.Named("metrics"),
		}
		traces = tracesBackup{
			client:   b.client,
			tables:   b.tables,
			database: db,
			logger:   b.logger.Named("traces"),
		}
		logs = logsBackup{
			client:   b.client,
			tables:   b.tables,
			database: db,
			logger:   b.logger.Named("logs"),
		}
	)

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx
		if err := metrics.Do(ctx, filepath.Join(dir, "metrics")); err != nil {
			return errors.Wrap(err, "backup metrics")
		}
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		if err := traces.Do(ctx, filepath.Join(dir, "traces")); err != nil {
			return errors.Wrap(err, "backup traces")
		}
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		if err := logs.Do(ctx, filepath.Join(dir, "logs")); err != nil {
			return errors.Wrap(err, "backup logs")
		}
		return nil
	})
	return grp.Wait()
}

func openBackupWriter(dir, name string) (io.WriteCloser, error) {
	dumpPath := filepath.Join(dir, name+".native.zstd")

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, errors.Wrap(err, "create dump directory")
	}

	f, err := os.Create(filepath.Clean(dumpPath))
	if err != nil {
		return nil, errors.Wrap(err, "create dump file")
	}

	enc, err := zstd.NewWriter(f)
	if err != nil {
		return nil, errors.Wrap(err, "make zstd encoder")
	}

	wc := struct {
		io.Writer
		io.Closer
	}{
		Writer: enc,
		Closer: closerFunc(func() error {
			encErr := enc.Close()
			fileErr := f.Close()
			return errors.Join(encErr, fileErr)
		}),
	}
	return wc, nil
}

type closerFunc func() error

var _ io.Closer = closerFunc(nil)

// Close implements [io.Closer].
func (c closerFunc) Close() error {
	return c()
}

func queryMinMaxTimestamp(ctx context.Context, client ClickHouseClient, tableColumns ...[2]string) (_, _ time.Time, _ error) {
	queryTable := func(ctx context.Context, table, column string) (mint, maxt time.Time, _ error) {
		var (
			minCol proto.ColDateTime
			maxCol proto.ColDateTime

			tsFunc = func(fn string) chsql.Expr {
				return chsql.ToDateTime(
					chsql.Function(fn, chsql.Ident(column)),
				)
			}
		)
		query := chsql.Select(table,
			chsql.ResultColumn{
				Name: "mint",
				Expr: tsFunc("min"),
				Data: &minCol,
			},
			chsql.ResultColumn{
				Name: "maxt",
				Expr: tsFunc("max"),
				Data: &maxCol,
			},
		)
		chq, err := query.Prepare(func(ctx context.Context, block proto.Block) error { return nil })
		if err != nil {
			return mint, maxt, errors.Wrap(err, "prepare query")
		}
		if err := client.Do(ctx, chq); err != nil {
			return mint, maxt, errors.Wrap(err, "execute query")
		}
		if len(minCol.Data) > 0 {
			mint = minCol.Data[0].Time()
			if mint.Unix() == 0 {
				mint = time.Time{}
			}
		}
		if len(maxCol.Data) > 0 {
			maxt = maxCol.Data[0].Time()
			if maxt.Unix() == 0 {
				maxt = time.Time{}
			}
		}
		return mint, maxt, nil
	}

	var (
		grp, grpCtx = errgroup.WithContext(ctx)
		mins        = make([]time.Time, len(tableColumns))
		maxs        = make([]time.Time, len(tableColumns))
	)
	for i, tc := range tableColumns {
		table := tc[0]
		column := tc[1]
		grp.Go(func() error {
			ctx := grpCtx
			mint, maxt, err := queryTable(ctx, table, column)
			if err != nil {
				return errors.Wrapf(err, "query %q.%q", table, column)
			}

			mins[i] = mint
			maxs[i] = maxt

			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return time.Time{}, time.Time{}, nil
	}

	var mint, maxt time.Time
	for _, t := range mins {
		if t.IsZero() {
			continue
		}
		if mint.IsZero() || t.Before(mint) {
			mint = t
		}
	}
	for _, t := range maxs {
		if t.IsZero() {
			continue
		}
		if maxt.IsZero() || t.After(maxt) {
			maxt = t
		}
	}

	switch {
	case mint.IsZero() && maxt.IsZero():
		// No data.
		return time.Time{}, time.Time{}, nil
	case mint.IsZero():
		return time.Time{}, time.Time{}, errors.New("unable to determine min timestamp")
	case maxt.IsZero():
		return time.Time{}, time.Time{}, errors.New("unable to determine max timestamp")
	default:
		return mint, maxt, nil
	}
}
