package odblogparser

import (
	"context"
	"fmt"
	"time"

	"github.com/go-faster/errors"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/multierr"

	"github.com/oteldb/oteldb/internal/logparser"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// Parser is a stanza operator that parses logs using oteldb internal log parsers.
type Parser struct {
	helper.ParserOperator

	format        logparser.Parser
	detectFormats []logparser.Parser
}

var _ operator.Operator = (*Parser)(nil)

// ProcessBatch parses a batch of entries.
func (p *Parser) ProcessBatch(ctx context.Context, entries []*entry.Entry) error {
	processedEntries := make([]*entry.Entry, 0, len(entries))
	write := func(_ context.Context, ent *entry.Entry) error {
		processedEntries = append(processedEntries, ent)
		return nil
	}

	var errs error
	for _, ent := range entries {
		if err := p.process(ctx, ent, write); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	errs = multierr.Append(errs, p.WriteBatch(ctx, processedEntries))
	return errs
}

// Process parses an entry.
func (p *Parser) Process(ctx context.Context, ent *entry.Entry) error {
	return p.process(ctx, ent, p.Write)
}

func (p *Parser) process(ctx context.Context, ent *entry.Entry, write helper.WriteFunction) error {
	skip, err := p.Skip(ctx, ent)
	if err != nil {
		return p.HandleEntryErrorWithWrite(ctx, ent, err, write)
	}
	if skip {
		return write(ctx, ent)
	}

	if err := p.parseEntry(ent); err != nil {
		return p.HandleEntryErrorWithWrite(ctx, ent, err, write)
	}
	return write(ctx, ent)
}

func (p *Parser) parseEntry(ent *entry.Entry) error {
	value, ok := ent.Get(p.ParseFrom)
	if !ok {
		return errors.Errorf("entry is missing parse_from field %s", p.ParseFrom.String())
	}

	data, ok := stringValue(value)
	if !ok {
		return errors.Errorf("type %T cannot be parsed, expected string or []byte", value)
	}

	record := recordFromEntry(ent, data)
	parser, err := p.selectParser(data)
	if err != nil {
		return err
	}
	if parser == nil {
		return nil
	}

	if err := parser.Parse(data, &record); err != nil {
		return err
	}
	if record.Attrs.IsZero() {
		record.Attrs = otelstorage.NewAttrs()
	}
	record.Attrs.AsMap().PutStr("logparser.type", parser.String())

	return applyRecord(ent, p.ParseTo, record)
}

func (p *Parser) selectParser(data string) (logparser.Parser, error) {
	if p.format != nil {
		return p.format, nil
	}
	for _, parser := range p.detectFormats {
		if parser.Detect(data) {
			return parser, nil
		}
	}
	return nil, nil
}

func stringValue(v any) (string, bool) {
	switch value := v.(type) {
	case string:
		return value, true
	case []byte:
		return string(value), true
	default:
		return "", false
	}
}

func recordFromEntry(ent *entry.Entry, body string) logparser.Record {
	return logparser.Record{
		ObservedTimestamp: otelstorage.NewTimestampFromTime(ent.ObservedTimestamp),
		Timestamp:         otelstorage.NewTimestampFromTime(ent.Timestamp),
		TraceID:           traceIDFromBytes(ent.TraceID),
		SpanID:            spanIDFromBytes(ent.SpanID),
		Flags:             flagsFromBytes(ent.TraceFlags),
		SeverityText:      ent.SeverityText,
		SeverityNumber:    stanzaToOTLPSeverity(ent.Severity),
		Body:              body,
		ScopeName:         ent.ScopeName,
	}
}

func applyRecord(ent *entry.Entry, parseTo entry.Field, record logparser.Record) error {
	if !record.ObservedTimestamp.AsTime().IsZero() {
		ent.ObservedTimestamp = record.ObservedTimestamp.AsTime()
	}
	if !record.Timestamp.AsTime().IsZero() {
		ent.Timestamp = record.Timestamp.AsTime()
	}
	if !record.TraceID.IsEmpty() {
		ent.TraceID = append(ent.TraceID[:0], record.TraceID[:]...)
	}
	if !record.SpanID.IsEmpty() {
		ent.SpanID = append(ent.SpanID[:0], record.SpanID[:]...)
	}
	if record.Flags != 0 {
		ent.TraceFlags = []byte{byte(record.Flags)}
	}
	if record.SeverityText != "" {
		ent.SeverityText = record.SeverityText
	}
	if record.SeverityNumber != 0 {
		ent.Severity = otlpToStanzaSeverity(record.SeverityNumber)
	}
	if record.ScopeName != "" {
		ent.ScopeName = record.ScopeName
	}
	if !record.ResourceAttrs.IsZero() && record.ResourceAttrs.Len() > 0 {
		mergeAttrsIntoMap(&ent.Resource, record.ResourceAttrs)
	}

	if isRootAttributesField(parseTo) {
		mergeAttrsIntoMap(&ent.Attributes, record.Attrs)
	} else if isRootResourceField(parseTo) {
		mergeAttrsIntoMap(&ent.Resource, record.Attrs)
	} else {
		if err := ent.Set(parseTo, mapFromAttrs(record.Attrs)); err != nil {
			return errors.Wrap(err, "set parse_to")
		}
	}
	if !isBodyField(parseTo) {
		ent.Body = record.Body
	}
	return nil
}

func stanzaToOTLPSeverity(severity entry.Severity) plog.SeverityNumber {
	switch severity {
	case entry.Trace, entry.Trace2, entry.Trace3, entry.Trace4,
		entry.Debug, entry.Debug2, entry.Debug3, entry.Debug4,
		entry.Info, entry.Info2, entry.Info3, entry.Info4,
		entry.Warn, entry.Warn2, entry.Warn3, entry.Warn4,
		entry.Error, entry.Error2, entry.Error3, entry.Error4,
		entry.Fatal, entry.Fatal2, entry.Fatal3, entry.Fatal4:
		return plog.SeverityNumber(severity)
	default:
		return plog.SeverityNumberUnspecified
	}
}

func otlpToStanzaSeverity(severity plog.SeverityNumber) entry.Severity {
	switch severity {
	case plog.SeverityNumberTrace, plog.SeverityNumberTrace2, plog.SeverityNumberTrace3, plog.SeverityNumberTrace4,
		plog.SeverityNumberDebug, plog.SeverityNumberDebug2, plog.SeverityNumberDebug3, plog.SeverityNumberDebug4,
		plog.SeverityNumberInfo, plog.SeverityNumberInfo2, plog.SeverityNumberInfo3, plog.SeverityNumberInfo4,
		plog.SeverityNumberWarn, plog.SeverityNumberWarn2, plog.SeverityNumberWarn3, plog.SeverityNumberWarn4,
		plog.SeverityNumberError, plog.SeverityNumberError2, plog.SeverityNumberError3, plog.SeverityNumberError4,
		plog.SeverityNumberFatal, plog.SeverityNumberFatal2, plog.SeverityNumberFatal3, plog.SeverityNumberFatal4:
		return entry.Severity(severity)
	default:
		return entry.Default
	}
}

func isBodyField(field entry.Field) bool {
	switch field.String() {
	case entry.NewBodyField().String(), "$body":
		return true
	default:
		return false
	}
}

func isRootAttributesField(field entry.Field) bool {
	switch field.String() {
	case entry.NewAttributeField().String(), "$attributes":
		return true
	default:
		return false
	}
}

func isRootResourceField(field entry.Field) bool {
	switch field.String() {
	case entry.NewResourceField().String(), "$resource":
		return true
	default:
		return false
	}
}

func mergeAttrsIntoMap(dst *map[string]any, attrs otelstorage.Attrs) {
	if attrs.IsZero() || attrs.Len() == 0 {
		return
	}
	if *dst == nil {
		*dst = make(map[string]any, attrs.Len())
	}
	attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
		(*dst)[k] = valueFromPcommon(v)
		return true
	})
}

func traceIDFromBytes(data []byte) (id otelstorage.TraceID) {
	copy(id[:], data)
	return id
}

func spanIDFromBytes(data []byte) (id otelstorage.SpanID) {
	copy(id[:], data)
	return id
}

func flagsFromBytes(data []byte) plog.LogRecordFlags {
	if len(data) == 0 {
		return 0
	}
	return plog.LogRecordFlags(data[0])
}

func attrsFromMap(src map[string]any) otelstorage.Attrs {
	attrs := otelstorage.NewAttrs()
	for k, v := range src {
		setValue(attrs.AsMap().PutEmpty(k), v)
	}
	return attrs
}

func setValue(dst pcommon.Value, src any) {
	switch value := src.(type) {
	case nil:
		return
	case string:
		dst.SetStr(value)
	case int:
		dst.SetInt(int64(value))
	case int8:
		dst.SetInt(int64(value))
	case int16:
		dst.SetInt(int64(value))
	case int32:
		dst.SetInt(int64(value))
	case int64:
		dst.SetInt(value)
	case uint:
		dst.SetInt(int64(value))
	case uint8:
		dst.SetInt(int64(value))
	case uint16:
		dst.SetInt(int64(value))
	case uint32:
		dst.SetInt(int64(value))
	case uint64:
		dst.SetInt(int64(value))
	case float32:
		dst.SetDouble(float64(value))
	case float64:
		dst.SetDouble(value)
	case bool:
		dst.SetBool(value)
	case []byte:
		dst.SetEmptyBytes().FromRaw(value)
	case time.Time:
		dst.SetStr(value.Format(time.RFC3339Nano))
	case map[string]any:
		m := dst.SetEmptyMap()
		for k, v := range value {
			setValue(m.PutEmpty(k), v)
		}
	case []any:
		slice := dst.SetEmptySlice()
		for _, v := range value {
			setValue(slice.AppendEmpty(), v)
		}
	case []string:
		slice := dst.SetEmptySlice()
		for _, v := range value {
			setValue(slice.AppendEmpty(), v)
		}
	case []int:
		slice := dst.SetEmptySlice()
		for _, v := range value {
			setValue(slice.AppendEmpty(), v)
		}
	case []int64:
		slice := dst.SetEmptySlice()
		for _, v := range value {
			setValue(slice.AppendEmpty(), v)
		}
	case []float64:
		slice := dst.SetEmptySlice()
		for _, v := range value {
			setValue(slice.AppendEmpty(), v)
		}
	case []bool:
		slice := dst.SetEmptySlice()
		for _, v := range value {
			setValue(slice.AppendEmpty(), v)
		}
	default:
		dst.SetStr(fmt.Sprint(value))
	}
}

func mapFromAttrs(attrs otelstorage.Attrs) map[string]any {
	if attrs.IsZero() {
		return nil
	}
	out := make(map[string]any, attrs.Len())
	attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
		out[k] = valueFromPcommon(v)
		return true
	})
	return out
}

func valueFromPcommon(v pcommon.Value) any {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeInt:
		return v.Int()
	case pcommon.ValueTypeDouble:
		return v.Double()
	case pcommon.ValueTypeBool:
		return v.Bool()
	case pcommon.ValueTypeBytes:
		return append([]byte(nil), v.Bytes().AsRaw()...)
	case pcommon.ValueTypeMap:
		out := make(map[string]any, v.Map().Len())
		v.Map().Range(func(k string, value pcommon.Value) bool {
			out[k] = valueFromPcommon(value)
			return true
		})
		return out
	case pcommon.ValueTypeSlice:
		slice := v.Slice()
		out := make([]any, 0, slice.Len())
		for i := 0; i < slice.Len(); i++ {
			out = append(out, valueFromPcommon(slice.At(i)))
		}
		return out
	case pcommon.ValueTypeEmpty:
		return nil
	default:
		return v.AsString()
	}
}
