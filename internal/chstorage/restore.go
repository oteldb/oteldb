package chstorage

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
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

func openBackupReader(dir, name string) (_ io.ReadCloser, rerr error) {
	dumpPath := filepath.Join(dir, name+".native.zstd")

	f, err := os.Open(filepath.Clean(dumpPath))
	if err != nil {
		return nil, errors.Wrap(err, "open dump file")
	}
	defer func() {
		if rerr != nil {
			_ = f.Close()
		}
	}()

	dec, err := zstd.NewReader(f)
	if err != nil {
		return nil, errors.Wrap(err, "make zstd decoder")
	}
	defer func() {
		if rerr != nil {
			dec.Close()
		}
	}()

	rc := struct {
		io.Reader
		io.Closer
	}{
		Reader: dec,
		Closer: closerFunc(func() error {
			dec.Close()
			return f.Close()
		}),
	}
	return rc, nil
}

type restoreTable struct {
	File       string
	NewColumns func() ([]proto.Input, Columns, func(), func() int)

	BatchSize  int
	BatchLimit int
	Logger     *zap.Logger
}

func (r *restoreTable) setDefaults() {
	if r.BatchSize == 0 {
		r.BatchSize = 1_000_000
	}
	if r.BatchLimit == 0 {
		r.BatchLimit = 100_000_000
	}
	if r.Logger == nil {
		r.Logger = zap.NewNop()
	}
}

func (r *restoreTable) Do(ctx context.Context, dir string, tables []string, client ClickHouseClient) error {
	r.setDefaults()

	decodeCh := make(chan []proto.Input)
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		defer close(decodeCh)
		return r.batcher(grpCtx, dir, decodeCh)
	})
	grp.Go(func() error {
		return r.inserter(grpCtx, tables, client, decodeCh)
	})
	return grp.Wait()
}

func (r *restoreTable) batcher(ctx context.Context, dir string, decodeCh chan<- []proto.Input) error {
	br, err := openBackupReader(dir, r.File)
	if err != nil {
		if os.IsNotExist(err) {
			r.Logger.Info("No backup found", zap.String("dir", dir), zap.String("file", r.File))
			return nil
		}
		return err
	}
	defer func() {
		_ = br.Close()
	}()

	var (
		block                      proto.Block
		rd                         = proto.NewReader(br)
		inputs, columns, add, rows = r.NewColumns()
	)
	for {
		columns.Reset()
		if err := block.DecodeRawBlock(rd, 54451, columns.Result()); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		add()
		total := rows()

		if total > r.BatchLimit {
			// Wait until inserter takes the buffer away.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case decodeCh <- inputs:
				inputs, columns, add, rows = r.NewColumns()
			}
		} else if total > r.BatchSize {
			// Try to insert block, if inserter have no work.
			// Otherwise, keep filling the buffer.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case decodeCh <- inputs:
				inputs, columns, add, rows = r.NewColumns()
			default:
			}
		}
	}
	if rows() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case decodeCh <- inputs:
		}
	}
	return nil
}

func (r *restoreTable) inserter(ctx context.Context, tables []string, client ClickHouseClient, decodeCh <-chan []proto.Input) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case inputs, ok := <-decodeCh:
			if !ok {
				return nil
			}
			grp, grpCtx := errgroup.WithContext(ctx)
			for i, input := range inputs {
				input := input
				table := tables[i]
				grp.Go(func() error {
					if err := client.Do(grpCtx, ch.Query{
						Body:  input.Into(table),
						Input: input,
					}); err != nil {
						return errors.Wrapf(err, "insert %s", table)
					}
					return nil
				})
			}
			if err := grp.Wait(); err != nil {
				return err
			}
		}
	}
}
