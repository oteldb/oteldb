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
	ctx             context.Context
	handler         *odbsafety.Handler[plog.LogRecord]
	records         plog.LogRecordSlice
	compacted       map[string]plog.LogRecord
	truncatedBySlot map[int64]plog.LogRecord
}

func newProcessBatch(ctx context.Context, handler *odbsafety.Handler[plog.LogRecord]) *processBatch {
	return &processBatch{ctx: ctx, handler: handler}
}

func (b *processBatch) handle(records plog.LogRecordSlice, record plog.LogRecord) bool {
	if !b.handler.Enabled() {
		return false
	}
	b.records = records
	return b.handler.Handle(b.ctx, b, record)
}

func (b *processBatch) Key(record plog.LogRecord, fields []string) string {
	return recordKey(record, fields)
}

func (b *processBatch) Time(record plog.LogRecord) time.Time {
	return recordTime(record)
}

func (b *processBatch) Truncate(slot int64, record plog.LogRecord, windowStart, windowEnd time.Time) {
	if b.truncatedBySlot == nil {
		b.truncatedBySlot = make(map[int64]plog.LogRecord, 1)
	}
	out, ok := b.truncatedBySlot[slot]
	if !ok {
		out = b.records.AppendEmpty()
		record.CopyTo(out)
		out.Body().SetStr("<output is truncated>")
		attrs := out.Attributes()
		attrs.PutInt("oteldb.truncated_count", 0)
		attrs.PutStr("oteldb.window_start", windowStart.Format(time.RFC3339Nano))
		attrs.PutStr("oteldb.window_end", windowEnd.Format(time.RFC3339Nano))
		b.truncatedBySlot[slot] = out
	}
	incrementIntAttr(out, "oteldb.truncated_count", 1)
}

func (b *processBatch) Compact(key string, record plog.LogRecord) {
	if b.compacted == nil {
		b.compacted = make(map[string]plog.LogRecord, 1)
	}
	out, ok := b.compacted[key]
	if !ok {
		out = b.records.AppendEmpty()
		record.CopyTo(out)
		out.Attributes().PutInt("oteldb.collapsed_count", 0)
		b.compacted[key] = out
	}
	incrementIntAttr(out, "oteldb.collapsed_count", 1)
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

func incrementIntAttr(record plog.LogRecord, key string, delta int64) {
	attrs := record.Attributes()
	value, ok := attrs.Get(key)
	if !ok {
		attrs.PutInt(key, delta)
		return
	}
	value.SetInt(value.Int() + delta)
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
