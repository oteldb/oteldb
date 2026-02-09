package logstorage

import (
	"context"
	"strings"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/oteldb/internal/logparser"
)

// Consumer consumes given logs and inserts them using given Inserter.
type Consumer struct {
	inserter Inserter
}

// NewConsumer creates new Consumer.
func NewConsumer(i Inserter) *Consumer {
	return &Consumer{
		inserter: i,
	}
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
				parsed := c.parseRecord(NewRecordFromOTEL(res, scope, record))
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

func (c *Consumer) parseRecord(record Record) Record {
	// NOTE(tdakkota): otelcol filelog receiver sends entries
	// 	with zero value timestamp
	//	Probably, this is the wrong way to handle it.
	//	Probably, we should not accept records if both timestamps are zero.
	ts := record.Timestamp
	if ts == 0 {
		ts = record.ObservedTimestamp
	}
	record.Timestamp = ts

	if record.Attrs.IsZero() || record.ResourceAttrs.IsZero() {
		return record
	}

	// Assuming filelog.
	// Should contain "log" attribute.
	attrs := record.Attrs.AsMap()
	const logMessageKey = "log"
	v, ok := attrs.Get(logMessageKey)
	if !ok || v.Type() != pcommon.ValueTypeStr {
		return record
	}
	for _, parser := range []logparser.Parser{
		logparser.GenericJSONParser{},
		// Disable for lots of false-positive detections // logparser.LogFmtParser{},
	} {
		if !parser.Detect(v.Str()) {
			continue
		}
		data := []byte(v.Str())
		line, err := parser.Parse(data)
		if err != nil {
			continue
		}
		attrs.PutStr("logparser.type", parser.String())
		attrs.Remove(logMessageKey)
		if !line.Attrs.IsZero() {
			line.Attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
				target := attrs.PutEmpty(k)
				v.CopyTo(target)
				return true
			})
		}
		record.Body = line.Body
		if line.Timestamp != 0 {
			record.Timestamp = line.Timestamp
		}
		record.SeverityNumber, record.SeverityText = normalizeSeverity(line.SeverityNumber, line.SeverityText)
		if !line.SpanID.IsEmpty() {
			record.SpanID = line.SpanID
		}
		if !line.TraceID.IsEmpty() {
			record.TraceID = line.TraceID
		}
		break
	}
	return record
}

func normalizeSeverity(number plog.SeverityNumber, text string) (_ plog.SeverityNumber, _ string) {
	switch {
	case number != 0 && text != "":
		return number, text
	case number != 0:
		text = number.String()
		return number, text
	case text != "":
		number = parseSeverity(text)
		return number, text
	default:
		return number, text
	}
}

func parseSeverity(text string) plog.SeverityNumber {
	normalized := false
retry:
	switch text {
	case "":
		return plog.SeverityNumberUnspecified
	case "trace", "TRACE":
		return plog.SeverityNumberTrace
	case "debug", "DEBUG":
		return plog.SeverityNumberDebug
	case "info", "INFO":
		return plog.SeverityNumberInfo
	case "warn", "WARN", "warning", "WARNING":
		return plog.SeverityNumberWarn
	case "error", "ERROR":
		return plog.SeverityNumberError
	case "fatal", "FATAL":
		return plog.SeverityNumberFatal
	default:
		if normalized {
			return plog.SeverityNumberUnspecified
		}
		text = strings.TrimSpace(text)
		text = strings.ToLower(text)
		normalized = true
		goto retry
	}
}
