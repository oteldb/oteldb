package logstorage

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/autometric"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/go-faster/oteldb/internal/logparser"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Consumer consumes given logs and inserts them using given Inserter.
type Consumer struct {
	inserter Inserter
	opts     ConsumerOptions
	stats    consumerStats
}

type consumerStats struct {
	ProcessedRecords    metric.Int64Counter `name:"records.processed.total" description:"Total number of processed log records" unit:"{records}"`
	ParseFailedRecords  metric.Int64Counter `name:"records.parse.failed" description:"Total number of processed log records that oteldb failed to parse" unit:"{records}"`
	ParseSuccessRecords metric.Int64Counter `name:"records.parse.success" description:"Total number of parsed log records" unit:"{records}"`

	ExplicitFormatRecords metric.Int64Counter `name:"records.explicit.total" description:"Total number of processed log records with explicitly defined format" unit:"{records}"`
	UnknownFormatRecords  metric.Int64Counter `name:"records.explicit.unknown" description:"Total number of processed log records with an explicitly specified but unknown format" unit:"{records}"`

	TotalDeducedRecords        metric.Int64Counter `name:"records.deduced.total" description:"Total number of log records that required format inference" unit:"{records}"`
	FailedDeducedRecords       metric.Int64Counter `name:"records.deduced.failed" description:"Total number of log records for which oteldb failed to deduce the format" unit:"{records}"`
	SuccessfullyDeducedRecords metric.Int64Counter `name:"records.deduced.success" description:"Total number of log records with successfully deduced format" unit:"{records}"`
}

func (s *consumerStats) Init(meter metric.Meter) error {
	return autometric.Init(meter, s, autometric.InitOptions{
		Prefix: "logstorage.consumer.",
	})
}

// NewConsumer creates new Consumer.
func NewConsumer(i Inserter, opts ConsumerOptions) (*Consumer, error) {
	opts.setDefaults()

	c := &Consumer{
		inserter: i,
		opts:     opts,
	}

	meter := opts.MeterProvider.Meter("logstorage.Consumer")
	if err := c.stats.Init(meter); err != nil {
		return nil, errors.Wrap(err, "init stats")
	}

	return c, nil
}

// ConsumeLogs implements otelreceiver.Consumer.
func (c *Consumer) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	w, err := c.inserter.RecordWriter(ctx)
	if err != nil {
		return errors.Wrap(err, "create record writer")
	}
	defer func() {
		_ = w.Close()
	}()

	resLogs := logs.ResourceLogs()
	for i := 0; i < resLogs.Len(); i++ {
		resLog := resLogs.At(i)
		res := resLog.Resource()

		scopeLogs := resLog.ScopeLogs()
		for i := 0; i < scopeLogs.Len(); i++ {
			scopeLog := scopeLogs.At(i)
			scope := scopeLog.Scope()

			records := scopeLog.LogRecords()
			for i := 0; i < records.Len(); i++ {
				record := records.At(i)
				parsed := c.processRecord(ctx, record.Body(), NewRecordFromOTEL(res, scope, record))
				if err := w.Add(parsed); err != nil {
					return errors.Wrap(err, "write record")
				}
			}
		}
	}

	if err := w.Submit(ctx); err != nil {
		return errors.Wrap(err, "submit log records")
	}
	return nil
}

func (c *Consumer) formatName(body pcommon.Value, record Record) (string, bool) {
	for _, attr := range c.opts.FormatAttributes {
		if v, ok := attr.Evaluate(body, record); ok {
			return v.AsString(), true
		}
	}
	return "", false
}

func (c *Consumer) trigger(body pcommon.Value, record Record) bool {
	for _, attr := range c.opts.TriggerAttributes {
		if _, ok := attr.Evaluate(body, record); ok {
			return true
		}
	}
	return false
}

func (c *Consumer) processRecord(ctx context.Context, body pcommon.Value, record Record) (result Record) {
	c.stats.ProcessedRecords.Add(ctx, 1)
	defer func() {
		result.SeverityNumber, result.SeverityText = normalizeSeverity(result.SeverityNumber, result.SeverityText)
	}()

	// NOTE(tdakkota): otelcol filelog receiver sends entries
	// 	with zero value timestamp
	// TODO(tdakkota): Probably, this is the wrong way to handle it.
	// TODO(tdakkota): Probably, we should not accept records if both timestamps are zero.
	ts := record.Timestamp
	if ts == 0 {
		ts = record.ObservedTimestamp
	}
	record.Timestamp = ts

	if record.Attrs.IsZero() {
		record.Attrs = otelstorage.NewAttrs()
	}
	if record.ResourceAttrs.IsZero() {
		record.ResourceAttrs = otelstorage.NewAttrs()
	}

	// Try to parse with specified format, if any.
	format, ok := c.formatName(body, record)
	if ok {
		c.stats.ExplicitFormatRecords.Add(ctx, 1, metric.WithAttributes(
			attribute.String("logstorage.format", format),
		))
		parser, ok := logparser.LookupFormat(format)
		if ok {
			parsed, err := c.parseRecord(parser, record.Body, record, false)
			if err == nil {
				c.stats.ParseSuccessRecords.Add(ctx, 1, metric.WithAttributes(
					attribute.String("logstorage.format", format),
					attribute.Bool("logstorage.deduced", false),
				))
				return parsed
			}
			c.stats.ParseFailedRecords.Add(ctx, 1, metric.WithAttributes(
				attribute.String("logstorage.format", format),
				attribute.Bool("logstorage.deduced", false),
			))
			return record
		}

		// If format is unknown, try to deduce it.
		c.stats.UnknownFormatRecords.Add(ctx, 1, metric.WithAttributes(
			attribute.String("logstorage.format", format),
		))
	}

	// But only if there are attributes triggering inference.
	if !c.trigger(body, record) {
		return record
	}
	c.stats.TotalDeducedRecords.Add(ctx, 1)

	// Try to deduce the format.
	for _, p := range c.opts.DetectFormats {
		if !p.Detect(record.Body) {
			continue
		}
		parsed, err := c.parseRecord(p, record.Body, record, true)
		if err == nil {
			c.stats.ParseSuccessRecords.Add(ctx, 1, metric.WithAttributes(
				attribute.String("logstorage.format", p.String()),
				attribute.Bool("logstorage.deduced", true),
			))
			c.stats.SuccessfullyDeducedRecords.Add(ctx, 1, metric.WithAttributes(
				attribute.String("logstorage.format", p.String()),
			))
			return parsed
		}
		c.stats.ParseFailedRecords.Add(ctx, 1, metric.WithAttributes(
			attribute.String("logstorage.format", p.String()),
			attribute.Bool("logstorage.deduced", true),
		))

		// Retry other detected formats, if this fails.
		// TODO(tdakkota): Use a pattern/decision tree. Say, try a Zap parser before generic JSON parser.
	}
	if len(c.opts.DetectFormats) > 0 {
		c.stats.FailedDeducedRecords.Add(ctx, 1)
	}
	return record
}

func (c *Consumer) parseRecord(parser logparser.Parser, data string, record Record, addType bool) (Record, error) {
	attrs := record.Attrs.AsMap()
	if err := parser.Parse(data, &record); err != nil {
		return record, err
	}

	if addType {
		attrs.PutStr("logparser.type", parser.String())
	}
	record.SeverityNumber, record.SeverityText = normalizeSeverity(record.SeverityNumber, record.SeverityText)
	return record, nil
}

func normalizeSeverity(number plog.SeverityNumber, text string) (_ plog.SeverityNumber, _ string) {
	switch {
	case number != 0 && text != "":
		return number, text
	case number != 0:
		text = number.String()
		return number, text
	case text != "":
		number = logparser.DeduceSeverity(text)
		return number, text
	default:
		return number, text
	}
}
