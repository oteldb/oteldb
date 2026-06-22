package odbsafetyprocessor

import (
	"context"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/odbsafety"
)

type processBatch struct {
	ctx     context.Context
	handler *odbsafety.Handler[plog.LogRecord]
	records plog.LogRecordSlice
}

func newProcessBatch(ctx context.Context, handler *odbsafety.Handler[plog.LogRecord]) *processBatch {
	return &processBatch{ctx: ctx, handler: handler}
}

func (b *processBatch) handle(mode string, records plog.LogRecordSlice, record plog.LogRecord) bool {
	if mode == "" {
		return false
	}
	b.records = records
	return b.handler.Handle(b.ctx, mode, b, record)
}

func (b *processBatch) Key(record plog.LogRecord, fields []string) string {
	return recordKey(record, fields)
}

func (b *processBatch) Time(record plog.LogRecord) time.Time {
	return recordTime(record)
}

func (b *processBatch) PassThrough(record plog.LogRecord) bool {
	if v, ok := record.Attributes().Get(odbsafety.PassthroughAttribute); ok && v.Type() == pcommon.ValueTypeBool {
		return v.Bool()
	}
	return false
}

func (b *processBatch) Clone(record plog.LogRecord) plog.LogRecord {
	out := plog.NewLogRecord()
	record.CopyTo(out)
	return out
}

func (b *processBatch) Truncate(_ int64, count int, record plog.LogRecord, windowStart, windowEnd time.Time) {
	out := b.records.AppendEmpty()
	record.CopyTo(out)
	out.Body().SetStr("<output is truncated>")
	attrs := out.Attributes()
	attrs.PutInt("oteldb.truncated_count", int64(count))
	attrs.PutStr("oteldb.window_start", windowStart.Format(time.RFC3339Nano))
	attrs.PutStr("oteldb.window_end", windowEnd.Format(time.RFC3339Nano))
}

func (b *processBatch) Compact(_ string, count int, record plog.LogRecord) {
	out := b.records.AppendEmpty()
	record.CopyTo(out)
	out.Attributes().PutInt("oteldb.collapsed_count", int64(count))
}

func recordTime(record plog.LogRecord) time.Time {
	if ts := record.Timestamp(); ts != 0 {
		return ts.AsTime()
	}
	if ts := record.ObservedTimestamp(); ts != 0 {
		return ts.AsTime()
	}
	return time.Now()
}

func recordKey(record plog.LogRecord, fields []string) string {
	if len(fields) == 0 {
		return valueKey(record.Body())
	}

	var b strings.Builder
	for _, field := range fields {
		b.WriteString(field)
		b.WriteByte('=')
		if field == "body" {
			b.WriteString(valueKey(record.Body()))
		} else if value, ok := record.Attributes().Get(field); ok {
			b.WriteString(valueKey(value))
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func valueKey(value pcommon.Value) string {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		return value.Str()
	case pcommon.ValueTypeInt:
		return strconv.FormatInt(value.Int(), 10)
	case pcommon.ValueTypeDouble:
		return strconv.FormatFloat(value.Double(), 'g', -1, 64)
	case pcommon.ValueTypeBool:
		return strconv.FormatBool(value.Bool())
	case pcommon.ValueTypeEmpty:
		return ""
	default:
		return value.AsString()
	}
}
