package tempohandler

import (
	"slices"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tempopb"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

type metadataCollector struct {
	limit     int
	metadatas map[otelstorage.TraceID]tempoapi.TraceSearchMetadata
}

func (b *metadataCollector) init() {
	if b.metadatas == nil {
		b.metadatas = make(map[otelstorage.TraceID]tempoapi.TraceSearchMetadata)
	}
}

func (b *metadataCollector) AddSpan(span tracestorage.Span) error {
	b.init()

	traceID := span.TraceID
	m, ok := b.metadatas[traceID]
	if !ok {
		if len(b.metadatas) >= b.limit {
			return nil
		}
		m = tempoapi.TraceSearchMetadata{
			TraceID: traceID.Hex(),
			SpanSet: tempoapi.NewOptTempoSpanSet(tempoapi.TempoSpanSet{}),
		}
	}
	ss := &m.SpanSet

	if span.ParentSpanID.IsEmpty() {
		span.FillTraceMetadata(&m)
	}
	ss.Value.Spans = append(ss.Value.Spans, span.AsTempoSpan())

	// Put modified struct back to map.
	b.metadatas[traceID] = m
	return nil
}

func (b *metadataCollector) Result() (r []tempoapi.TraceSearchMetadata) {
	r = make([]tempoapi.TraceSearchMetadata, 0, len(b.metadatas))
	for _, v := range b.metadatas {
		// Skip trace, if we have not got parent span yet.
		if v.StartTimeUnixNano.IsZero() {
			continue
		}
		r = append(r, v)
	}
	slices.SortFunc(r, func(a, b tempoapi.TraceSearchMetadata) int {
		return a.StartTimeUnixNano.Compare(b.StartTimeUnixNano)
	})
	return r
}

type spanKey struct {
	batchID      uuid.UUID
	scopeName    string
	scopeVersion string
}

type batchCollector struct {
	spanCount  int
	resSpans   map[uuid.UUID]*tracev1.ResourceSpans
	scopeSpans map[spanKey]*tracev1.ScopeSpans
}

func (b *batchCollector) init() {
	if b.resSpans == nil {
		b.resSpans = make(map[uuid.UUID]*tracev1.ResourceSpans)
	}
	if b.scopeSpans == nil {
		b.scopeSpans = make(map[spanKey]*tracev1.ScopeSpans)
	}
}

func (b *batchCollector) getScopeSpans(s tracestorage.Span) *tracev1.ScopeSpans {
	b.init()

	k := spanKey{
		batchID:      s.BatchID,
		scopeName:    s.ScopeName,
		scopeVersion: s.ScopeVersion,
	}
	if ss, ok := b.scopeSpans[k]; ok {
		return ss
	}

	resSpan, ok := b.resSpans[s.BatchID]
	if !ok {
		resSpan = &tracev1.ResourceSpans{
			Resource: &resourcev1.Resource{
				Attributes: attrsToProto(s.ResourceAttrs),
			},
		}
		b.resSpans[s.BatchID] = resSpan
	}

	scopeSpan := &tracev1.ScopeSpans{
		Scope: &commonv1.InstrumentationScope{
			Name:       s.ScopeName,
			Version:    s.ScopeVersion,
			Attributes: attrsToProto(s.ScopeAttrs),
		},
	}
	resSpan.ScopeSpans = append(resSpan.ScopeSpans, scopeSpan)
	b.scopeSpans[k] = scopeSpan
	return scopeSpan
}

// AddSpan adds the span to the batch.
func (b *batchCollector) AddSpan(span tracestorage.Span) error {
	ss := b.getScopeSpans(span)
	ss.Spans = append(ss.Spans, spanToProto(span))
	b.spanCount++
	return nil
}

// SpanCount returns the total number of collected spans.
func (b *batchCollector) SpanCount() int {
	return b.spanCount
}

func (b *batchCollector) resourceSpans() []*tracev1.ResourceSpans {
	result := make([]*tracev1.ResourceSpans, 0, len(b.resSpans))
	for _, rs := range b.resSpans {
		result = append(result, rs)
	}
	return result
}

// ResultExport returns an ExportTraceServiceRequest for the TraceByID (v1) endpoint.
func (b *batchCollector) ResultExport() *tempopb.ExportTraceServiceRequest {
	return &tempopb.ExportTraceServiceRequest{
		ResourceSpans: b.resourceSpans(),
	}
}

// ResultTempo returns a TraceByIDResponse for the TraceByIDv2 endpoint.
func (b *batchCollector) ResultTempo() *tempopb.TraceByIDResponse {
	return &tempopb.TraceByIDResponse{
		Trace: &tempopb.Trace{
			ResourceSpans: b.resourceSpans(),
		},
	}
}

func spanToProto(span tracestorage.Span) *tracev1.Span {
	s := &tracev1.Span{
		TraceId:           span.TraceID[:],
		SpanId:            span.SpanID[:],
		TraceState:        span.TraceState,
		Name:              span.Name,
		Kind:              tracev1.Span_SpanKind(span.Kind),
		StartTimeUnixNano: uint64(span.Start),
		EndTimeUnixNano:   uint64(span.End),
		Attributes:        attrsToProto(span.Attrs),
		Status: &tracev1.Status{
			Code:    tracev1.Status_StatusCode(span.StatusCode),
			Message: span.StatusMessage,
		},
	}
	if p := span.ParentSpanID; !p.IsEmpty() {
		s.ParentSpanId = p[:]
	}
	for _, event := range span.Events {
		s.Events = append(s.Events, &tracev1.Span_Event{
			TimeUnixNano: uint64(event.Timestamp),
			Name:         event.Name,
			Attributes:   attrsToProto(event.Attrs),
		})
	}
	for _, link := range span.Links {
		s.Links = append(s.Links, &tracev1.Span_Link{
			TraceId:    link.TraceID[:],
			SpanId:     link.SpanID[:],
			TraceState: link.TraceState,
			Attributes: attrsToProto(link.Attrs),
		})
	}
	return s
}

func attrsToProto(attrs otelstorage.Attrs) []*commonv1.KeyValue {
	m := attrs.AsMap()
	if m.Len() == 0 {
		return nil
	}
	result := make([]*commonv1.KeyValue, 0, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		result = append(result, &commonv1.KeyValue{
			Key:   k,
			Value: valueToProto(v),
		})
		return true
	})
	return result
}

func valueToProto(v pcommon.Value) *commonv1.AnyValue {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v.Str()}}
	case pcommon.ValueTypeBool:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: v.Bool()}}
	case pcommon.ValueTypeInt:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: v.Int()}}
	case pcommon.ValueTypeDouble:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: v.Double()}}
	case pcommon.ValueTypeBytes:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: v.Bytes().AsRaw()}}
	case pcommon.ValueTypeSlice:
		arr := v.Slice()
		values := make([]*commonv1.AnyValue, arr.Len())
		for i := range arr.Len() {
			values[i] = valueToProto(arr.At(i))
		}
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{
			ArrayValue: &commonv1.ArrayValue{Values: values},
		}}
	case pcommon.ValueTypeMap:
		kvs := attrsToProto(otelstorage.Attrs(v.Map()))
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{
			KvlistValue: &commonv1.KeyValueList{Values: kvs},
		}}
	default:
		return &commonv1.AnyValue{}
	}
}
