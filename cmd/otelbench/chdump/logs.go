package chdump

import (
	"iter"
	"slices"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Logs is a parsed dump of logs.
type Logs struct {
	ServiceInstanceID *proto.ColLowCardinality[string]
	ServiceName       *proto.ColLowCardinality[string]
	ServiceNamespace  *proto.ColLowCardinality[string]

	Timestamp *proto.ColDateTime64

	SeverityNumber *proto.ColUInt8
	SeverityText   *proto.ColLowCardinality[string]

	TraceFlags *proto.ColUInt8
	TraceID    *proto.ColRawOf[otelstorage.TraceID]
	SpanID     *proto.ColRawOf[otelstorage.SpanID]

	Body *proto.ColStr

	Attributes *Attributes
	Resource   *Attributes

	Scope        *Attributes
	ScopeName    *proto.ColLowCardinality[string]
	ScopeVersion *proto.ColLowCardinality[string]
}

// NewLogs creates a new [Logs].
func NewLogs() *Logs {
	c := &Logs{
		ServiceInstanceID: new(proto.ColStr).LowCardinality(),
		ServiceName:       new(proto.ColStr).LowCardinality(),
		ServiceNamespace:  new(proto.ColStr).LowCardinality(),

		Timestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),

		SeverityText:   new(proto.ColStr).LowCardinality(),
		SeverityNumber: new(proto.ColUInt8),

		TraceFlags: new(proto.ColUInt8),
		TraceID:    new(proto.ColRawOf[otelstorage.TraceID]),
		SpanID:     new(proto.ColRawOf[otelstorage.SpanID]),

		Body: new(proto.ColStr),

		Attributes: NewAttributes(),
		Resource:   NewAttributes(),

		Scope:        NewAttributes(),
		ScopeName:    new(proto.ColStr).LowCardinality(),
		ScopeVersion: new(proto.ColStr).LowCardinality(),
	}

	return c
}

// Result returns a [Logs] result.
func (c *Logs) Result() proto.Results {
	return slices.Collect(c.columns())
}

// Reset resets [Logs] data.
func (c *Logs) Reset() {
	for col := range c.columns() {
		col.Data.Reset()
	}
}

// ToOTLP appends data from [Logs] to given batch.
func (c *Logs) ToOTLP(batch plog.Logs) {
	resMap := map[otelstorage.Hash]plog.ResourceLogs{}
	resLogs := batch.ResourceLogs()
	for i := range resLogs.Len() {
		resLog := resLogs.At(i)
		attrs := otelstorage.Attrs(resLog.Resource().Attributes())
		resMap[attrs.Hash()] = resLog
	}

	getResLog := func(resourceAttrs otelstorage.Attrs) plog.ResourceLogs {
		hash := resourceAttrs.Hash()

		resLog, ok := resMap[hash]
		if !ok {
			resLog = resLogs.AppendEmpty()
			resource := resLog.Resource()
			resourceAttrs.AsMap().CopyTo(resource.Attributes())

			resMap[hash] = resLog
		}
		return resLog
	}
	getScopeLog := func(resLog plog.ResourceLogs, scopeAttrs otelstorage.Attrs, scopeName, scopeVersion string) plog.ScopeLogs {
		scopeLogs := resLog.ScopeLogs()
		scopeAttrsHash := scopeAttrs.Hash()

		for i := range scopeLogs.Len() {
			scopeLog := scopeLogs.At(i)
			scope := scopeLog.Scope()
			if scope.Name() == scopeName &&
				scope.Version() == scopeVersion &&
				otelstorage.Attrs(scope.Attributes()).Hash() == scopeAttrsHash {
				return scopeLog
			}
		}
		scopeLog := scopeLogs.AppendEmpty()

		scope := scopeLog.Scope()
		scope.SetName(scopeName)
		scope.SetVersion(scopeVersion)
		scopeAttrs.AsMap().CopyTo(scope.Attributes())

		return scopeLog
	}

	for row := range c.Body.Rows() {
		timestamp := c.Timestamp.Row(row)
		severityText := c.SeverityText.Row(row)
		severityNumber := c.SeverityNumber.Row(row)
		traceFlags := c.TraceFlags.Row(row)
		traceID := c.TraceID.Row(row)
		spanID := c.SpanID.Row(row)
		body := c.Body.Row(row)
		attributes := c.Attributes.Row(row)
		resource := c.Resource.Row(row)
		scope := c.Scope.Row(row)
		scopeName := c.ScopeName.Row(row)
		scopeVersion := c.ScopeVersion.Row(row)

		resLog := getResLog(resource)
		scopeLog := getScopeLog(resLog, scope, scopeName, scopeVersion)
		record := scopeLog.LogRecords().AppendEmpty()

		record.SetTimestamp(otelstorage.NewTimestampFromTime(timestamp))
		record.SetSeverityText(severityText)
		record.SetSeverityNumber(plog.SeverityNumber(severityNumber))
		record.SetFlags(plog.LogRecordFlags(traceFlags))
		record.SetTraceID(pcommon.TraceID(traceID))
		record.SetSpanID(pcommon.SpanID(spanID))
		record.Body().SetStr(body)
		attributes.CopyTo(record.Attributes())
	}
}

func (c *Logs) columns() iter.Seq[proto.ResultColumn] {
	return func(yield func(proto.ResultColumn) bool) {
		for _, col := range []proto.ResultColumn{
			{Name: "service_instance_id", Data: c.ServiceInstanceID},
			{Name: "service_name", Data: c.ServiceName},
			{Name: "service_namespace", Data: c.ServiceNamespace},

			{Name: "timestamp", Data: c.Timestamp},

			{Name: "severity_text", Data: c.SeverityText},
			{Name: "severity_number", Data: c.SeverityNumber},

			{Name: "trace_id", Data: c.TraceID},
			{Name: "span_id", Data: c.SpanID},
			{Name: "trace_flags", Data: c.TraceFlags},

			{Name: "body", Data: c.Body},

			{Name: "attribute", Data: c.Attributes},
			{Name: "resource", Data: c.Resource},

			{Name: "scope", Data: c.Scope},
			{Name: "scope_name", Data: c.ScopeName},
			{Name: "scope_version", Data: c.ScopeVersion},
		} {
			if !yield(col) {
				return
			}
		}
	}
}
