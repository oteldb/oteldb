package chstorage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
)

type metricsBackup struct {
	client   ClickHouseClient
	tables   Tables
	database string

	logger *zap.Logger
}

func (b *metricsBackup) Do(ctx context.Context, dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return errors.Wrap(err, "create directory")
	}

	mint, maxt, err := queryMinMaxTimestamp(ctx, b.client,
		[2]string{b.tables.Points, "timestamp"},
		[2]string{b.tables.ExpHistograms, "timestamp"},
		[2]string{b.tables.Exemplars, "timestamp"},
	)
	if err != nil {
		return errors.Wrap(err, "query min/max timestamp")
	}
	if mint.IsZero() && maxt.IsZero() {
		b.logger.Info("No metrics to backup")
		return nil
	}

	var (
		step  = 24 * time.Hour
		start = mint.Truncate(step)
		end   = maxt.Truncate(step).Add(step)
	)
	for ts := start; ts.Before(end); ts = ts.Add(step) {
		if err := b.backup(ctx, dir, ts, ts.Add(step)); err != nil {
			return err
		}
	}
	return nil
}

func (b *metricsBackup) backup(ctx context.Context, root string, start, end time.Time) error {
	var (
		stopwatch = time.Now()
		dir       = filepath.Join(root, start.Format("2006-01-02_15-04-05"))
	)
	b.logger.Info("Backing up metrics", zap.Time("start", start))

	tsColumns, err := b.queryExistingColumns(ctx, b.tables.Timeseries)
	if err != nil {
		return err
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx
		if err := b.backupPoints(ctx, dir, start, end, tsColumns); err != nil {
			return errors.Wrap(err, "backup points")
		}
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		if err := b.backupExpHistograms(ctx, dir, start, end, tsColumns); err != nil {
			return errors.Wrap(err, "backup exp histograms")
		}
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		if err := b.backupExemplars(ctx, dir, start, end, tsColumns); err != nil {
			return errors.Wrap(err, "backup exp histograms")
		}
		return nil
	})
	if err := grp.Wait(); err != nil {
		return err
	}

	b.logger.Info("Backed up metrics", zap.Duration("took", time.Since(stopwatch)), zap.String("dir", dir))
	return nil
}

func (b *metricsBackup) backupPoints(ctx context.Context, dir string, start, end time.Time, tsColumns map[string]struct{}) error {
	table := b.tables.Points
	w, err := openBackupWriter(dir, "metrics_points")
	if err != nil {
		return err
	}
	defer func() {
		_ = w.Close()
	}()
	columns := backupColumns{
		{Name: "timestamp", Data: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)},
		{Name: "value", Data: new(proto.ColFloat64)},

		{Name: "mapping", Data: new(proto.ColEnum8)},
		{Name: "flags", Data: new(proto.ColUInt8), Default: chsql.Integer[uint8](0)},
	}
	q, input, err := b.buildBackupQuery(ctx, table, columns, start, end, tsColumns)
	if err != nil {
		return err
	}
	if err := b.saveBackup(ctx, q, input, w); err != nil {
		return err
	}
	return nil
}

func (b *metricsBackup) backupExpHistograms(ctx context.Context, dir string, start, end time.Time, tsColumns map[string]struct{}) error {
	table := b.tables.ExpHistograms
	w, err := openBackupWriter(dir, "metrics_exp_histograms")
	if err != nil {
		return err
	}
	defer func() {
		_ = w.Close()
	}()
	columns := backupColumns{
		{Name: "timestamp", Data: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)},
		{Name: "exp_histogram_count", Data: new(proto.ColUInt64)},
		{Name: "exp_histogram_sum", Data: new(proto.ColFloat64).Nullable()},
		{Name: "exp_histogram_min", Data: new(proto.ColFloat64).Nullable()},
		{Name: "exp_histogram_max", Data: new(proto.ColFloat64).Nullable()},
		{Name: "exp_histogram_scale", Data: new(proto.ColInt32)},
		{Name: "exp_histogram_zerocount", Data: new(proto.ColUInt64)},
		{Name: "exp_histogram_positive_offset", Data: new(proto.ColInt32)},
		{Name: "exp_histogram_positive_bucket_counts", Data: new(proto.ColUInt64).Array()},
		{Name: "exp_histogram_negative_offset", Data: new(proto.ColInt32)},
		{Name: "exp_histogram_negative_bucket_counts", Data: new(proto.ColUInt64).Array()},

		{Name: "flags", Data: new(proto.ColUInt8)},
	}
	q, input, err := b.buildBackupQuery(ctx, table, columns, start, end, tsColumns)
	if err != nil {
		return err
	}
	if err := b.saveBackup(ctx, q, input, w); err != nil {
		return err
	}
	return nil
}

func (b *metricsBackup) backupExemplars(ctx context.Context, dir string, start, end time.Time, tsColumns map[string]struct{}) error {
	table := b.tables.Exemplars
	w, err := openBackupWriter(dir, "metrics_exemplars")
	if err != nil {
		return err
	}
	defer func() {
		_ = w.Close()
	}()
	columns := backupColumns{
		{Name: "timestamp", Data: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)},

		{Name: "filtered_attributes", Data: new(proto.ColBytes)},
		{Name: "exemplar_timestamp", Data: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)},
		{Name: "value", Data: new(proto.ColFloat64)},
		{Name: "span_id", Data: new(proto.ColFixedStr8)},
		{Name: "trace_id", Data: new(proto.ColFixedStr16)},
	}
	q, input, err := b.buildBackupQuery(ctx, table, columns, start, end, tsColumns)
	if err != nil {
		return err
	}
	if err := b.saveBackup(ctx, q, input, w); err != nil {
		return err
	}
	return nil
}

func (b *metricsBackup) saveBackup(ctx context.Context, q *chsql.SelectQuery, input proto.Input, w io.Writer) error {
	var buf proto.Buffer
	chq, err := q.Prepare(func(ctx context.Context, block proto.Block) error {
		buf.Reset()
		if err := block.EncodeRawBlock(&buf, 54451, input); err != nil {
			return errors.Wrap(err, "encode raw block")
		}
		if _, err := w.Write(buf.Buf); err != nil {
			return errors.Wrap(err, "write block")
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "prepare query")
	}
	if err := b.client.Do(ctx, chq); err != nil {
		return errors.Wrap(err, "do query")
	}
	return nil
}

func (b *metricsBackup) buildBackupQuery(ctx context.Context, table string, columns backupColumns, start, end time.Time, existingTSColumns map[string]struct{}) (*chsql.SelectQuery, proto.Input, error) {
	const tsAlias = "ts"

	tsColumns := b.timeseriesColumns(tsAlias)
	input := slices.Concat(columns, tsColumns).Input()

	existingColumns, err := b.queryExistingColumns(ctx, table)
	if err != nil {
		return nil, nil, err
	}

	dataResult, err := columns.ChsqlResult(existingColumns)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "build expressions for %q", table)
	}
	tsResult, err := tsColumns.ChsqlResult(existingTSColumns)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "build expressions for %q", tsAlias)
	}
	results := slices.Concat(dataResult, tsResult)

	query := chsql.Select(table, results...).
		Alias("t").
		InnerJoin(b.tables.Timeseries, tsAlias,
			chsql.Eq(
				chsql.PrefixedIdent("t", "hash"),
				chsql.PrefixedIdent(tsAlias, "hash"),
			),
		).
		Where(
			chsql.Gte(chsql.Ident("timestamp"), chsql.DateTime(start)),
			chsql.Lte(chsql.Ident("timestamp"), chsql.DateTime(end)),
		)
	return query, input, nil
}

func (b *metricsBackup) queryExistingColumns(ctx context.Context, table string) (map[string]struct{}, error) {
	sourceColumns, err := queryTableColumns(ctx, b.client, b.database, table)
	if err != nil {
		return nil, errors.Wrapf(err, "query %s.%s columns", b.database, table)
	}
	existingColumns := make(map[string]struct{}, len(sourceColumns))
	for _, dc := range sourceColumns {
		existingColumns[dc.Name] = struct{}{}
	}
	return existingColumns, nil
}

func (b *metricsBackup) timeseriesColumns(table string) backupColumns {
	return backupColumns{
		{
			Name:    "name",
			Expr:    chsql.Cast(chsql.PrefixedIdent(table, "name"), "LowCardinality(String)"),
			Data:    &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)},
			Default: chsql.Cast(chsql.String(""), "LowCardinality(String)"),
		},
		{
			Name:    "unit",
			Expr:    chsql.Cast(chsql.PrefixedIdent(table, "unit"), "LowCardinality(String)"),
			Data:    &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)},
			Default: chsql.Cast(chsql.String(""), "LowCardinality(String)"),
		},
		{
			Name:    "description",
			Expr:    chsql.Cast(chsql.PrefixedIdent(table, "description"), "String"),
			Data:    new(proto.ColStr),
			Default: chsql.String(""),
		},

		{Name: "attribute", Expr: chsql.PrefixedIdent(table, "attribute"), Data: &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}},
		{Name: "scope", Expr: chsql.PrefixedIdent(table, "scope"), Data: &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}},
		{Name: "resource", Expr: chsql.PrefixedIdent(table, "resource"), Data: &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}},
	}
}
