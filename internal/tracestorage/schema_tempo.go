package tracestorage

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/tempoapi"
)

// FillTraceMetadata files TraceSearchMetadata fields using span.
//
// The span should be a parent span.
func (span Span) FillTraceMetadata(m *tempoapi.TraceSearchMetadata) {
	ss := &m.SpanSet.Value

	m.RootTraceName.SetTo(span.Name)
	if name, ok := span.ServiceName(); ok {
		m.RootServiceName.SetTo(name)
	}
	var (
		start = time.Unix(0, int64(span.Start))
		end   = time.Unix(0, int64(span.End))
	)

	m.StartTimeUnixNano = start
	m.DurationMs.SetTo(int(end.Sub(start).Milliseconds()))
	ConvertToTempoAttrs(&ss.Attributes, span.ScopeAttrs)
	ConvertToTempoAttrs(&ss.Attributes, span.ResourceAttrs)
}

// AsTempoSpan converts span to TempoSpan.
func (span Span) AsTempoSpan() (s tempoapi.TempoSpan) {
	s = tempoapi.TempoSpan{
		SpanID:            span.SpanID.Hex(),
		Name:              tempoapi.NewOptString(span.Name),
		StartTimeUnixNano: time.Unix(0, int64(span.Start)),
		DurationNanos:     int64(span.End - span.Start),
		Attributes:        nil,
	}
	ConvertToTempoAttrs(&s.Attributes, span.Attrs)
	return s
}

// AsTempoSpanFiltered is like [Span.AsTempoSpan], but only attributes whose
// key is in allowed are converted.
func (span Span) AsTempoSpanFiltered(allowed map[string]struct{}) (s tempoapi.TempoSpan) {
	s = tempoapi.TempoSpan{
		SpanID:            span.SpanID.Hex(),
		Name:              tempoapi.NewOptString(span.Name),
		StartTimeUnixNano: time.Unix(0, int64(span.Start)),
		DurationNanos:     int64(span.End - span.Start),
		Attributes:        nil,
	}
	ConvertToTempoAttrsFiltered(&s.Attributes, span.Attrs, allowed)
	return s
}

// ConvertToTempoAttrs converts [otelstorage.Attrs] to Tempo API attributes.
func ConvertToTempoAttrs(to *tempoapi.Attributes, from otelstorage.Attrs) {
	if from.IsZero() {
		return
	}
	m := from.AsMap()
	*to = slices.Grow(*to, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		*to = append(*to, tempoapi.KeyValue{
			Key:   k,
			Value: otelToTempoValue(v),
		})
		return true
	})
}

// ConvertToTempoAttrsFiltered is like [ConvertToTempoAttrs], but only
// attributes whose key is in allowed are converted.
func ConvertToTempoAttrsFiltered(to *tempoapi.Attributes, from otelstorage.Attrs, allowed map[string]struct{}) {
	if from.IsZero() || len(allowed) == 0 {
		return
	}
	m := from.AsMap()
	m.Range(func(k string, v pcommon.Value) bool {
		if _, ok := allowed[k]; !ok {
			return true
		}
		*to = append(*to, tempoapi.KeyValue{
			Key:   k,
			Value: otelToTempoValue(v),
		})
		return true
	})
}

func otelToTempoValue(val pcommon.Value) (r tempoapi.AnyValue) {
	switch val.Type() {
	case pcommon.ValueTypeStr:
		r.SetStringValue(tempoapi.StringValue{StringValue: val.Str()})
	case pcommon.ValueTypeBool:
		r.SetBoolValue(tempoapi.BoolValue{BoolValue: val.Bool()})
	case pcommon.ValueTypeInt:
		r.SetIntValue(tempoapi.IntValue{IntValue: val.Int()})
	case pcommon.ValueTypeDouble:
		r.SetDoubleValue(tempoapi.DoubleValue{DoubleValue: val.Double()})
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		// Grafana search result transformation does not handle nested attributes
		// and panics if it encounters one.
		//
		// See:
		//  - https://github.com/grafana/grafana/blob/v13.0.1/pkg/tsdb/tempo/search.go#L569
		//  - https://github.com/grafana/grafana/blob/v13.0.1/pkg/tsdb/tempo/search.go#L540
		data, err := json.Marshal(val.AsRaw())
		if err != nil {
			r.SetStringValue(tempoapi.StringValue{StringValue: fmt.Sprintf("marshal %q: %s", val.Type(), err.Error())})
		} else {
			r.SetStringValue(tempoapi.StringValue{StringValue: string(data)})
		}
	case pcommon.ValueTypeBytes:
		r.SetBytesValue(tempoapi.BytesValue{BytesValue: val.Bytes().AsRaw()})
	default:
		r.Type = tempoapi.StringValueAnyValue
	}
	return r
}
