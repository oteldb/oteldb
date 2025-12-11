package chstorage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

type logsRestore struct {
	client ClickHouseClient
	tables Tables

	logger *zap.Logger
}

func (r *logsRestore) Do(ctx context.Context, root string) error {
	dirs, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			r.logger.Info("No logs to restore")
			return nil
		}
		return err
	}
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		step := filepath.Join(root, d.Name())
		if err := r.restore(ctx, step); err != nil {
			return errors.Wrapf(err, "restore dir %q", step)
		}
	}
	return nil
}

func (r *logsRestore) restore(ctx context.Context, dir string) error {
	stopwatch := time.Now()
	r.logger.Info("Restoring logs dir", zap.String("dir", dir))

	if err := r.restoreLogs(ctx, dir); err != nil {
		return errors.Wrap(err, "restore logs")
	}

	r.logger.Info("Restored logs dir", zap.Duration("took", time.Since(stopwatch)), zap.String("dir", dir))
	return nil
}

func (r *logsRestore) restoreLogs(ctx context.Context, dir string) error {
	w, err := openBackupReader(dir, "logs")
	if err != nil {
		if os.IsNotExist(err) {
			r.logger.Info("No logs backup found", zap.String("dir", dir))
			return nil
		}
		return err
	}
	defer func() { _ = w.Close() }()

	var (
		timestamp = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)

		severityNumber proto.ColUInt8
		severityText   = new(proto.ColStr).LowCardinality()

		traceID    proto.ColRawOf[otelstorage.TraceID]
		spanID     proto.ColRawOf[otelstorage.SpanID]
		traceFlags proto.ColUInt8

		body proto.ColStr

		scopeName    = new(proto.ColStr).LowCardinality()
		scopeVersion = new(proto.ColStr).LowCardinality()

		attributes = NewAttributes(colAttrs, WithLowCardinality(false))
		scope      = NewAttributes(colScope)
		resource   = NewAttributes(colResource)

		columns = MergeColumns(
			Columns{
				{Name: "timestamp", Data: timestamp},

				{Name: "severity_number", Data: &severityNumber},
				{Name: "severity_text", Data: severityText},

				{Name: "trace_id", Data: &traceID},
				{Name: "span_id", Data: &spanID},
				{Name: "trace_flags", Data: &traceFlags},

				{Name: "body", Data: &body},

				{Name: "scope_name", Data: scopeName},
				{Name: "scope_version", Data: scopeVersion},
			},
			attributes.Columns(),
			scope.Columns(),
			resource.Columns(),
		)

		block proto.Block
		rd    = proto.NewReader(w)
	)

	for {
		columns.Reset()
		if err := block.DecodeRawBlock(rd, 54451, columns.Result()); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			if err != nil {
				return err
			}
			break
		}

		var (
			lc = newLogColumns()
			ac = newLogAttrMapColumns()
		)
		for i := 0; i < timestamp.Rows(); i++ {
			var rec logstorage.Record
			rec.Timestamp = otelstorage.NewTimestampFromTime(timestamp.Row(i))
			rec.SeverityNumber = plog.SeverityNumber(severityNumber.Row(i))
			rec.SeverityText = severityText.Row(i)

			rec.TraceID = traceID.Row(i)
			rec.SpanID = spanID.Row(i)

			rec.Flags = plog.LogRecordFlags(traceFlags.Row(i))
			rec.Body = body.Row(i)

			rec.ScopeName = scopeName.Row(i)
			rec.ScopeVersion = scopeVersion.Row(i)

			rec.Attrs = attributes.Row(i)
			rec.ScopeAttrs = scope.Row(i)
			rec.ResourceAttrs = resource.Row(i)

			rec.ObservedTimestamp = rec.Timestamp

			lc.AddRow(rec)
			ac.AddAttrs(rec.Attrs)
			ac.AddAttrs(rec.ScopeAttrs)
			ac.AddAttrs(rec.ResourceAttrs)
		}

		grp, grpCtx := errgroup.WithContext(ctx)
		grp.Go(func() error {
			ctx := grpCtx

			input := lc.Input()
			if err := r.client.Do(ctx, ch.Query{
				Body:  input.Into(r.tables.Logs),
				Input: input,
			}); err != nil {
				return errors.Wrap(err, "insert logs")
			}
			return nil
		})
		grp.Go(func() error {
			ctx := grpCtx

			input := ac.Input()
			if err := r.client.Do(ctx, ch.Query{
				Body:  input.Into(r.tables.LogAttrs),
				Input: input,
			}); err != nil {
				return errors.Wrap(err, "insert log attrs")
			}
			return nil
		})
		if err := grp.Wait(); err != nil {
			return err
		}
	}
	return nil
}
