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
)

type logsBackup struct {
	client ClickHouseClient
	tables Tables
	logger *zap.Logger
}

func (b *logsBackup) Do(ctx context.Context, dir string) error {
	mint, maxt, err := queryMinMaxTimestamp(ctx, b.client,
		[2]string{b.tables.Logs, "timestamp"},
	)
	if err != nil {
		return errors.Wrap(err, "query min/max timestamp")
	}

	step := 24 * time.Hour
	start := mint.Truncate(step)
	end := maxt.Truncate(step).Add(step)
	for ts := start; ts.Before(end); ts = ts.Add(step) {
		if err := b.backup(ctx, dir, ts, ts.Add(step)); err != nil {
			return err
		}
	}
	return nil
}

func (b *logsBackup) backup(ctx context.Context, root string, start, end time.Time) error {
	var (
		stopwatch = time.Now()
		dir       = filepath.Join(root, start.Format("2006-01-02_15-04-05"))
	)
	b.logger.Info("Backing up logs dir", zap.Time("start", start))

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return errors.Wrap(err, "create directory")
	}

	if err := b.backupLogs(ctx, dir, start, end); err != nil {
		return errors.Wrap(err, "backup logs")
	}

	b.logger.Info("Backed up logs dir", zap.Duration("took", time.Since(stopwatch)), zap.String("dir", dir))
	return nil
}

func (b *logsBackup) backupLogs(ctx context.Context, dir string, start, end time.Time) error {
	table := b.tables.Logs
	w, err := openBackupWriter(dir, "logs")
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()

	var (
		timestamp = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)

		severityNumber proto.ColUInt8
		severityText   = new(proto.ColStr).LowCardinality()

		traceID    proto.ColFixedStr16
		spanID     proto.ColFixedStr8
		traceFlags proto.ColUInt8

		body proto.ColStr

		scopeName    = new(proto.ColStr).LowCardinality()
		scopeVersion = new(proto.ColStr).LowCardinality()

		attribute = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		scope     = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
		resource  = &proto.ColLowCardinalityRaw{Index: new(proto.ColStr)}
	)

	columns := MergeColumns(
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
		Columns{
			{Name: "attribute", Data: attribute},
			{Name: "scope", Data: scope},
			{Name: "resource", Data: resource},
		},
	)

	var buf proto.Buffer

	q := fmt.Sprintf(`SELECT
	timestamp,
	severity_number,
	severity_text,
	trace_id,
	span_id,
	trace_flags,
	body,
	scope_name,
	scope_version,
	attribute,
	scope,
	resource
FROM %s
WHERE timestamp >= toDateTime(%d) AND timestamp <= toDateTime(%d)`, table, start.Unix(), end.Unix())

	if err := b.client.Do(ctx, ch.Query{
		Body:   q,
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
