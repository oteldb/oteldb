package chstorage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type metricsBackup struct {
	client ClickHouseClient
	tables Tables
	logger *zap.Logger
}

func (b *metricsBackup) Do(ctx context.Context, dir string) error {
	mint, maxt, err := queryMinMaxTimestamp(ctx, b.client,
		[2]string{b.tables.Points, "timestamp"},
		[2]string{b.tables.ExpHistograms, "timestamp"},
		[2]string{b.tables.Exemplars, "timestamp"},
	)
	if err != nil {
		return errors.Wrap(err, "query min/max timestamp")
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
	b.logger.Info("Backing up metrics dir", zap.Time("start", start))

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return errors.Wrap(err, "create directory")
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx
		if err := b.backupPoints(ctx, dir, start, end); err != nil {
			return errors.Wrap(err, "backup points")
		}
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		if err := b.backupExpHistograms(ctx, dir, start, end); err != nil {
			return errors.Wrap(err, "backup exp histograms")
		}
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		if err := b.backupExemplars(ctx, dir, start, end); err != nil {
			return errors.Wrap(err, "backup exp histograms")
		}
		return nil
	})
	if err := grp.Wait(); err != nil {
		return err
	}

	b.logger.Info("Backed up metrics dir", zap.Duration("took", time.Since(stopwatch)), zap.String("dir", dir))
	return nil
}

func (b *metricsBackup) backupPoints(ctx context.Context, dir string, start, end time.Time) error {
	table := b.tables.Points
	w, err := openBackupWriter(dir, "metrics_points")
	if err != nil {
		return err
	}
	defer func() {
		_ = w.Close()
	}()
	var (
		timestamp = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		value     proto.ColFloat64

		mapping proto.ColEnum8
		flags   proto.ColUInt8

		name        = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		unit        = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		description proto.ColStr

		attribute = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		scope     = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		resource  = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}

		columns = MergeColumns(
			Columns{
				{Name: "timestamp", Data: timestamp},
				{Name: "value", Data: &value},

				{Name: "mapping", Data: proto.Wrap(&mapping, metricMappingDDL)},
				{Name: "flags", Data: &flags},
			},
			Columns{
				{Name: "name", Data: name},
				{Name: "unit", Data: unit},
				{Name: "description", Data: &description},

				{Name: "attribute", Data: attribute},
				{Name: "scope", Data: scope},
				{Name: "resource", Data: resource},
			},
		)
		buf proto.Buffer
	)
	if err := b.client.Do(ctx, ch.Query{
		Body: fmt.Sprintf(`SELECT
  timestamp,
  value,
  mapping,
  flags,
  ts.name AS name,
  toString(ts.unit) AS unit,
  toString(ts.description) AS description,
  ts.attribute AS attribute,
  ts.scope AS scope,
  ts.resource AS resource
FROM
  `+table+` p
  INNER JOIN metrics_timeseries ts ON p.hash = ts.hash
WHERE timestamp >= toDateTime(%d) AND timestamp <= toDateTime(%d)`, start.Unix(), end.Unix()),
		Result: columns.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			buf.Reset()
			if err := block.EncodeRawBlock(&buf, 54451, columns.Input()); err != nil {
				return errors.Wrap(err, "encode raw block")
			}
			if _, err := w.Write(buf.Buf); err != nil {
				return errors.Wrap(err, "write block")
			}
			return nil
		},
	}); err != nil {
		return err
	}
	return nil
}

func (b *metricsBackup) backupExpHistograms(ctx context.Context, dir string, start, end time.Time) error {
	table := b.tables.ExpHistograms
	w, err := openBackupWriter(dir, "metrics_exp_histograms")
	if err != nil {
		return err
	}
	defer func() {
		_ = w.Close()
	}()
	var (
		timestamp            = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		count                proto.ColUInt64
		sum                  = new(proto.ColFloat64).Nullable()
		cmin                 = new(proto.ColFloat64).Nullable()
		cmax                 = new(proto.ColFloat64).Nullable()
		scale                proto.ColInt32
		zerocount            proto.ColUInt64
		positiveOffset       proto.ColInt32
		positiveBucketCounts = new(proto.ColUInt64).Array()
		negativeOffset       proto.ColInt32
		negativeBucketCounts = new(proto.ColUInt64).Array()

		flags proto.ColUInt8

		name        = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		unit        = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		description proto.ColStr

		attribute = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		scope     = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		resource  = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}

		columns = MergeColumns(
			Columns{
				{Name: "timestamp", Data: timestamp},
				{Name: "exp_histogram_count", Data: &count},
				{Name: "exp_histogram_sum", Data: sum},
				{Name: "exp_histogram_min", Data: cmin},
				{Name: "exp_histogram_max", Data: cmax},
				{Name: "exp_histogram_scale", Data: &scale},
				{Name: "exp_histogram_zerocount", Data: &zerocount},
				{Name: "exp_histogram_positive_offset", Data: &positiveOffset},
				{Name: "exp_histogram_positive_bucket_counts", Data: positiveBucketCounts},
				{Name: "exp_histogram_negative_offset", Data: &negativeOffset},
				{Name: "exp_histogram_negative_bucket_counts", Data: negativeBucketCounts},

				{Name: "flags", Data: &flags},
			},
			Columns{
				{Name: "name", Data: name},
				{Name: "unit", Data: unit},
				{Name: "description", Data: &description},

				{Name: "attribute", Data: attribute},
				{Name: "scope", Data: scope},
				{Name: "resource", Data: resource},
			},
		)
		buf proto.Buffer
	)
	if err := b.client.Do(ctx, ch.Query{
		Body: fmt.Sprintf(`SELECT
  timestamp,
  exp_histogram_count,
  exp_histogram_sum,
  exp_histogram_min,
  exp_histogram_max,
  exp_histogram_scale,
  exp_histogram_zerocount,
  exp_histogram_positive_offset,
  exp_histogram_positive_bucket_counts,
  exp_histogram_negative_offset,
  exp_histogram_negative_bucket_counts,
  flags,
  ts.name AS name,
  toString(ts.unit) AS unit,
  toString(ts.description) AS description,
  ts.attribute AS attribute,
  ts.scope AS scope,
  ts.resource AS resource
FROM
  `+table+` p
  INNER JOIN metrics_timeseries ts ON p.hash = ts.hash
WHERE timestamp >= toDateTime(%d) AND timestamp <= toDateTime(%d)`, start.Unix(), end.Unix()),
		Result: columns.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			buf.Reset()
			if err := block.EncodeRawBlock(&buf, 54451, columns.Input()); err != nil {
				return errors.Wrap(err, "encode raw block")
			}
			if _, err := w.Write(buf.Buf); err != nil {
				return errors.Wrap(err, "write block")
			}
			return nil
		},
	}); err != nil {
		return err
	}
	return nil
}

func (b *metricsBackup) backupExemplars(ctx context.Context, dir string, start, end time.Time) error {
	table := b.tables.Exemplars
	w, err := openBackupWriter(dir, "metrics_exemplars")
	if err != nil {
		return err
	}
	defer func() {
		_ = w.Close()
	}()
	var (
		timestamp = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)

		filteredAttributes proto.ColBytes
		exemplarTimestamp  = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		value              proto.ColFloat64
		spanID             proto.ColFixedStr8
		traceID            proto.ColFixedStr16

		name        = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		unit        = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		description proto.ColStr

		attribute = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		scope     = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		resource  = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}

		columns = MergeColumns(
			Columns{
				{Name: "timestamp", Data: timestamp},

				{Name: "filtered_attributes", Data: &filteredAttributes},
				{Name: "exemplar_timestamp", Data: exemplarTimestamp},
				{Name: "value", Data: &value},
				{Name: "span_id", Data: &spanID},
				{Name: "trace_id", Data: &traceID},
			},
			Columns{
				{Name: "name", Data: name},
				{Name: "unit", Data: unit},
				{Name: "description", Data: &description},

				{Name: "attribute", Data: attribute},
				{Name: "scope", Data: scope},
				{Name: "resource", Data: resource},
			},
		)
		buf proto.Buffer
	)
	if err := b.client.Do(ctx, ch.Query{
		Body: fmt.Sprintf(`SELECT
  timestamp,
  filtered_attributes,
  exemplar_timestamp,
  value,
  span_id,
  trace_id,
  ts.name AS name,
  toString(ts.unit) AS unit,
  toString(ts.description) AS description,
  ts.attribute AS attribute,
  ts.scope AS scope,
  ts.resource AS resource
FROM
  `+table+` p
  INNER JOIN metrics_timeseries ts ON p.hash = ts.hash
WHERE timestamp >= toDateTime(%d) AND timestamp <= toDateTime(%d)`, start.Unix(), end.Unix()),
		Result: columns.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			buf.Reset()
			if err := block.EncodeRawBlock(&buf, 54451, columns.Input()); err != nil {
				return errors.Wrap(err, "encode raw block")
			}
			if _, err := w.Write(buf.Buf); err != nil {
				return errors.Wrap(err, "write block")
			}
			return nil
		},
	}); err != nil {
		return err
	}
	return nil
}
